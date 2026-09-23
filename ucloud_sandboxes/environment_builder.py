"""Build immutable components from a fresh allowlisted view of OCI image input.

The only public input is the existing immutable Docker image adapter. No runtime
workspace, memory directory, or checkpoint is accepted as a publication source.
"""
from dataclasses import dataclass
import os
from pathlib import Path, PurePosixPath
import shutil
import stat
import subprocess
from tempfile import TemporaryDirectory

from .environment_artifact import sign_component
from .image_rootfs import DockerOverlay2RootfsStore


def _copy_metadata(source, destination, info):
    if hasattr(os, "listxattr"):
        for name in os.listxattr(source, follow_symlinks=False):
            os.setxattr(destination, name, os.getxattr(source, name, follow_symlinks=False), follow_symlinks=False)
    os.chown(destination, info.st_uid, info.st_gid, follow_symlinks=False)
    if not stat.S_ISLNK(info.st_mode):
        os.chmod(destination, stat.S_IMODE(info.st_mode))
    os.utime(destination, ns=(info.st_atime_ns, info.st_mtime_ns), follow_symlinks=False)


def _copy_entry(source, destination, hardlinks):
    info = source.lstat()
    if stat.S_ISDIR(info.st_mode):
        destination.mkdir(mode=0o700)
        for child in sorted(source.iterdir(), key=lambda path: path.name):
            # This is a mounted immutable filesystem, not an OCI layer tar.
            # A literal .wh.* filename is ordinary user data. Real filesystem
            # whiteout devices and opaque xattrs retain their exact semantics.
            _copy_entry(child, destination / child.name, hardlinks)
    elif stat.S_ISREG(info.st_mode):
        identity = (info.st_dev, info.st_ino)
        previous = hardlinks.get(identity)
        if previous is not None:
            os.link(previous, destination)
        else:
            # Input is an immutable image lease; NOFOLLOW also fences a replaced
            # final path and the post-read stat rejects a changing builder view.
            descriptor = os.open(source, os.O_RDONLY | os.O_NOFOLLOW)
            with os.fdopen(descriptor, "rb") as reader, destination.open("xb") as writer:
                current = os.fstat(reader.fileno())
                if (current.st_dev, current.st_ino) != identity:
                    raise ValueError("immutable build input changed")
                shutil.copyfileobj(reader, writer, 1024 * 1024)
                after = os.fstat(reader.fileno())
                if (after.st_size, after.st_mtime_ns) != (info.st_size, info.st_mtime_ns):
                    raise ValueError("immutable build input changed while copying")
            hardlinks[identity] = destination
    elif stat.S_ISLNK(info.st_mode):
        destination.symlink_to(os.readlink(source))
    elif stat.S_ISCHR(info.st_mode) and info.st_rdev == os.makedev(0, 0):
        os.mknod(destination, stat.S_IFCHR | 0o600, info.st_rdev)
    else:
        raise ValueError("environment build view contains an unsupported special file")
    _copy_metadata(source, destination, info)


def allowlisted_build_view(source_root: Path, destination: Path, paths):
    """Copy declared immutable image paths, preserving merged-layer semantics."""
    if source_root.is_symlink() or not source_root.is_dir() or destination.exists():
        raise ValueError("fresh environment view requires a real source and new destination")
    allowed = []
    for value in paths:
        if not isinstance(value, str):
            raise ValueError("environment allowlist paths must be strings")
        path = PurePosixPath(value)
        if (path.is_absolute() or not path.parts
                or any(part in {".", ".."} for part in path.parts) or "\0" in value):
            raise ValueError("environment allowlist paths must stay within the immutable image")
        if path not in allowed:
            allowed.append(path)
    if not allowed:
        raise ValueError("an explicit environment publication allowlist is required")
    allowed = sorted(path for path in allowed if not any(parent in allowed for parent in path.parents))
    destination.mkdir(mode=0o700)
    hardlinks = {}
    for path in allowed:
        source = source_root
        target = destination
        for part in path.parts[:-1]:
            source /= part
            target /= part
            if source.is_symlink() or not source.is_dir():
                raise ValueError("allowlist traverses a non-directory or symlink")
            target.mkdir(mode=0o700, exist_ok=True)
        source /= path.name
        if not source.exists() and not source.is_symlink():
            raise ValueError("allowlisted build path is absent")
        _copy_entry(source, target / path.name, hardlinks)
    # Parents synthesized for narrow allowlist entries also retain source
    # ownership/mode/xattrs; do this last so readonly directories remain writable
    # while the fresh view is being assembled.
    for path in sorted(destination.rglob("*"), key=lambda p: len(p.parts), reverse=True):
        original = source_root / path.relative_to(destination)
        if path.is_dir() and not path.is_symlink() and original.is_dir():
            _copy_metadata(original, path, original.lstat())
    _copy_metadata(source_root, destination, source_root.lstat())


@dataclass
class FreshEnvironmentBuilder:
    image_store: DockerOverlay2RootfsStore
    registry: object
    signing_key: object
    work_root: Path
    mkfs_erofs: str = "mkfs.erofs"

    def build(self, image_ref, *, allowlist, tag):
        if not isinstance(self.image_store, DockerOverlay2RootfsStore):
            raise ValueError("environment publication requires the immutable OCI build adapter")
        self.work_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        image_id = None
        try:
            with self.image_store.operation_lease(image_ref) as source:
                image_id = source.image_id
                with TemporaryDirectory(dir=self.work_root) as temporary:
                    root = Path(temporary)
                    view, image = root / "view", root / "component.erofs"
                    allowlisted_build_view(source.rootfs, view, allowlist)
                    subprocess.run((self.mkfs_erofs, "-T", "0", "-U", "00000000-0000-0000-0000-000000000000",
                                    str(image), str(view)), check=True, capture_output=True, timeout=600)
                    component = sign_component(image, source_image=source.image_id, signing_key=self.signing_key)
                    digest = self.registry.publish(image, component, tag=tag)
                    return {"image_id": source.image_id, "component_digest": digest,
                            "component": component, "image_config": source.image_config}
        finally:
            if image_id is not None:
                # Builders have no sandbox registry users of this temporary
                # mount. Existing image leases fence concurrent build readers.
                self.image_store.collect_image(image_id, is_referenced=lambda _: False)

    def publish_image(self, image_ref, *, allowlist, toolkits=()):
        """Publish fresh build output and attach it to the existing image tag."""
        import uuid
        from .environment_artifact import attach_environment_to_image, publish_environment
        from .environment_manifest import EnvironmentManifest
        from .managed_registry import registry_repository_tag_from_image_ref
        coordinates = registry_repository_tag_from_image_ref(image_ref)
        if coordinates is None or "@" in image_ref:
            raise ValueError("environment publication requires an owned image tag")
        repository, tag = coordinates
        # Buildx direct-push deliberately leaves no local image. Pull this
        # completed immutable build input before constructing the fresh view.
        self.image_store._checked(self.image_store.docker_binary, "pull", image_ref, timeout=600)
        result = self.build(image_ref, allowlist=allowlist, tag="environment-component-" + uuid.uuid4().hex)
        config = result["image_config"]
        environment_digest = publish_environment(self.registry,
            source_image=result["image_id"], environment=EnvironmentManifest(result["component_digest"], toolkits=tuple(toolkits)),
            image_config={"Entrypoint": list(config.entrypoint), "Cmd": list(config.command), "Env": list(config.env),
                          "WorkingDir": config.working_dir, "User": config.user},
            signing_key=self.signing_key, tag="environment-root-" + uuid.uuid4().hex)
        return attach_environment_to_image(self.registry, image_repository=repository, image_reference=tag,
                                           environment_digest=environment_digest)


def main(argv=None):
    """Publish one canary through the same builder implementation as image builds."""
    import argparse
    from .environment_config import add_environment_registry_args
    parser = argparse.ArgumentParser(description="Publish a fresh allowlisted immutable image artifact")
    parser.add_argument("--image-ref", required=True)
    parser.add_argument("--state-root", type=Path, required=True)
    parser.add_argument("--docker-binary", default="docker")
    parser.add_argument("--environment-signing-key", type=Path, required=True)
    parser.add_argument("--environment-allow-path", action="append", default=[])
    add_environment_registry_args(parser)
    args = parser.parse_args(argv)
    return publish_from_args(args)


def publish_from_args(args):
    import json
    from types import SimpleNamespace
    from .environment_artifact import load_image_environment
    from .environment_config import environment_publisher_from_args, environment_registry_from_args
    from .managed_registry import registry_repository_tag_from_image_ref
    args.image_file = args.state_root / "images.sqlite"
    publisher = environment_publisher_from_args(args)
    digest = publisher(SimpleNamespace(tag=args.image_ref))
    repository, _ = registry_repository_tag_from_image_ref(args.image_ref)
    root, environment = load_image_environment(environment_registry_from_args(args), repository, digest)
    print(json.dumps({"image_ref": args.image_ref + "@" + digest, "manifest_digest": digest,
                      "environment_root": root, "components": environment.components}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
