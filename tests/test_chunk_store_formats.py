"""Chunk store formats: round trips, bounds and tamper detection (no I/O)."""
from dataclasses import replace
import hashlib
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from tests.chunk_store_support import pseudo_random, signing, write_bootstrap
from ucloud_sandboxes.chunk_store import (PACK_ENTRY, PACK_TRAILER, RAW, ZSTD, ChunkMap, Locator, PackWriter,
                                          chunk_map_from_bootstrap, decode_chunk, parse_bootstrap, parse_pack_tail,
                                          store_encoding, zstd_compress, zstd_decompress)
from ucloud_sandboxes.environment_artifact import (CHUNK_BYTES, EnvironmentComponent, RafsEnvironmentComponent,
                                                   bind_rafs_layers, sign_rafs_component)


def sha(data):
    return hashlib.sha256(data).digest()


class CodecTests(unittest.TestCase):
    def test_zstd_is_exact_and_capped(self):
        data = pseudo_random("text", 1000) * 200
        packed = zstd_compress(data)
        self.assertEqual(zstd_decompress(packed, len(data)), data)
        for size in (len(data) - 1, len(data) + 1):
            with self.assertRaises(ValueError):
                zstd_decompress(packed, size)
        with self.assertRaises(ValueError):
            zstd_decompress(packed[:-3], len(data))

    def test_store_encoding_keeps_zstd_only_when_it_pays(self):
        text, noise = bytes(CHUNK_BYTES), pseudo_random("noise", CHUNK_BYTES)
        packed = zstd_compress(text)
        self.assertEqual(store_encoding(packed, CHUNK_BYTES, 0x11), (packed, ZSTD))
        # Under 4 KiB, or zstd saving under 3%: stored raw, never recompressed.
        small = b"a" * 1000
        self.assertEqual(store_encoding(zstd_compress(small), 1000, 0x11), (small, RAW))
        self.assertEqual(store_encoding(zstd_compress(noise), CHUNK_BYTES, 0x11), (noise, RAW))
        self.assertEqual(store_encoding(noise, CHUNK_BYTES, 0x10), (noise, RAW))

    def test_decode_verifies_the_uncompressed_id(self):
        data = b"x" * 9000
        self.assertEqual(decode_chunk(zstd_compress(data), 9000, ZSTD, sha(data)), data)
        for payload, size, flags, digest in ((data, 9000, RAW, sha(b"y")), (data[:-1], 9000, RAW, sha(data)),
                                             (zstd_compress(data), 9000, 7, sha(data))):
            with self.assertRaises(ValueError):
                decode_chunk(payload, size, flags, digest)


class PackTests(unittest.TestCase):
    def write(self, chunks):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        writer = PackWriter(Path(directory.name) / "pack")
        for data in chunks:
            writer.add(sha(data), data, len(data), RAW)
        digest, size = writer.finish()
        payload = writer.path.read_bytes()
        self.assertEqual((hashlib.sha256(payload).hexdigest(), size), (digest, len(payload)))
        return payload

    def test_round_trip_footer_sorted_by_id_data_in_address_order(self):
        chunks = [pseudo_random(seed, 5000 + seed) for seed in range(20)]
        payload = self.write(chunks)
        entries = parse_pack_tail(payload, len(payload))
        self.assertEqual([entry[0] for entry in entries], sorted(sha(data) for data in chunks))
        by_id = {entry[0]: entry for entry in entries}
        offsets = [by_id[sha(data)][1] for data in chunks]
        self.assertEqual(offsets, sorted(offsets))
        for data in chunks:
            _, offset, clen, ulen, flags = by_id[sha(data)]
            self.assertEqual(decode_chunk(payload[offset:offset + clen], ulen, flags, sha(data)), data)
        # A short tail asks for the length the trailer names.
        self.assertIsNone(parse_pack_tail(payload[-PACK_TRAILER.size - 10:], len(payload)))

    def test_tampered_footers_are_rejected(self):
        payload = bytearray(self.write([b"a" * 100, b"b" * 200]))
        footer = len(payload) - PACK_TRAILER.size - 2 * PACK_ENTRY.size
        for position in (footer + 33, len(payload) - 40, len(payload) - 1):
            changed = bytearray(payload)
            changed[position] ^= 1
            with self.subTest(position=position), self.assertRaises(ValueError):
                parse_pack_tail(bytes(changed), len(changed))

    def test_writer_bounds_each_pack(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        writer = PackWriter(Path(directory.name) / "pack")
        self.assertTrue(writer.fits(CHUNK_BYTES))
        self.assertFalse(writer.fits(64 * 1024 ** 2))
        with self.assertRaises(ValueError):
            writer.add(sha(b"a"), b"a", 2, RAW)  # Raw payloads are exactly the chunk.
        writer.finish()


def bootstrap(blocks=(100, 3), chunk_sizes=((4096, 9), (300,))):
    devices, chunks, mapped = [], [], 128
    for index, (count, sizes) in enumerate(zip(blocks, chunk_sizes)):
        devices.append((hashlib.sha256(bytes([index])).hexdigest(), count, mapped))
        offset = 0
        for size in sizes:
            data = pseudo_random(f"{index}:{offset}", size)
            chunks.append((sha(data), index, 0x10, size, size, offset, offset))
            offset += -(-size // 4096) * 4096
        mapped += -(-count // 128) * 128
    return write_bootstrap(devices, chunks)


class ChunkMapTests(unittest.TestCase):
    def test_derived_from_the_chunk_table_and_round_trips(self):
        parsed = parse_bootstrap(bootstrap())
        chunk_map = chunk_map_from_bootstrap(parsed)
        self.assertEqual(chunk_map.offsets, (128 * 4096, 129 * 4096, 256 * 4096))
        self.assertEqual(chunk_map.device_size, (256 + 3) * 4096)
        self.assertEqual(ChunkMap.decode(chunk_map.encode()), chunk_map)
        self.assertTrue(chunk_map.matches(parsed))
        self.assertEqual(list(chunk_map.overlapping(129 * 4096 + 5, 256 * 4096 + 1)), [1, 2])

    def test_structure_is_validated(self):
        chunk_map = chunk_map_from_bootstrap(parse_bootstrap(bootstrap()))
        for changes in ({"offsets": (128 * 4096, 128 * 4096 + 1, 256 * 4096)},  # unaligned, overlapping
                        {"offsets": (129 * 4096, 128 * 4096, 256 * 4096)},  # unsorted
                        {"sizes": (4096, CHUNK_BYTES + 1, 300)},
                        {"offsets": (128 * 4096, 228 * 4096, 256 * 4096)},  # between regions
                        {"device_size": chunk_map.device_size + 4096},
                        {"regions": ((chunk_map.regions[0][0], 1, 100), chunk_map.regions[1])}):  # over the bootstrap
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                replace(chunk_map, **changes)
        encoded = chunk_map.encode()
        for broken in (encoded[:-1], b"X" + encoded[1:], encoded + b"\0"):
            with self.assertRaises(ValueError):
                ChunkMap.decode(broken)
        other = parse_bootstrap(bootstrap(blocks=(100, 4)))
        self.assertFalse(chunk_map.matches(other))

    def test_bootstrap_parser_refuses_unqualified_tables(self):
        data = bytearray(bootstrap())
        for position, value in ((1024, 0), (1024 + 12, 9), (1024 + 128, 0x4), (1024 + 128 + 20, 0x10000)):
            changed = bytearray(data)
            changed[position:position + 4] = value.to_bytes(4, "little")
            with self.subTest(position=position), self.assertRaises(ValueError):
                parse_bootstrap(bytes(changed))
        with self.assertRaises(ValueError):
            parse_bootstrap(bytes(data[:-1]))


class LocatorTests(unittest.TestCase):
    def test_round_trip_and_bounds(self):
        locator = Locator(3, (("a" * 64, "https://s3/a?x=1"),), ((0, 16, 100, RAW), (0, 116, 50, ZSTD)),
                          {"bootstrap": "https://s3/b", "chunk_map": "https://s3/m"})
        self.assertEqual(Locator.decode(locator.encode()), locator)
        for changes in ({"entries": ((1, 16, 100, RAW),)}, {"entries": ((0, 0, 100, RAW),)},
                        {"entries": ((0, 16, 100, 2),)}, {"packs": (("a" * 64, "file:///etc/passwd"),)},
                        {"meta": {"other": "https://s3"}}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                replace(locator, **changes)
        with self.assertRaises(ValueError):
            Locator.decode(locator.encode()[:-1])


class RafsComponentTests(unittest.TestCase):
    def setUp(self):
        self.key, self.trusted = signing()
        self.layers = ["sha256:" + "1" * 64, "sha256:" + "2" * 64]

    def sign(self, layout="image", **changes):
        values = {"source_image": "sha256:" + "c" * 64, "source_layers": self.layers,
                  "bootstrap": {"digest": "sha256:" + "b" * 64, "size": 8192},
                  "chunk_map": {"digest": "sha256:" + "d" * 64, "size": 44}, "device_size": 4096 * 300,
                  "layout": layout, "signing_key": self.key} | changes
        return sign_rafs_component(**values)

    def test_signed_round_trip_and_tamper_detection(self):
        component = self.sign()
        parsed = EnvironmentComponent.from_dict(component.to_dict())
        self.assertIsInstance(parsed, RafsEnvironmentComponent)
        self.assertEqual(parsed.authenticate(self.trusted), component)
        for name, value in (("chunk_map", {"digest": "sha256:" + "e" * 64, "size": 44}), ("device_size", 4096),
                            ("source_layers", tuple(reversed(self.layers)))):
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, "signature"):
                replace(component, **{name: value}).authenticate(self.trusted)
        with self.assertRaises(ValueError):
            RafsEnvironmentComponent.from_dict(component.to_dict() | {"extra": 1})
        with self.assertRaises(ValueError):
            RafsEnvironmentComponent.from_dict(component.to_dict() | {"format": component.format | {"rafs": 5}})
        with self.assertRaises(ValueError):
            self.sign(device_size=4097)
        with self.assertRaises(ValueError):
            self.sign(layout="layer")  # A layer component names no image and one layer.

    def test_root_binding_rebuilds_exactly_the_image_layers(self):
        image = self.sign()
        bind_rafs_layers([image], image.source_image, self.layers)
        layers = [self.sign("layer", source_image=None, source_layers=[digest]) for digest in self.layers]
        bind_rafs_layers(layers, "sha256:" + "f" * 64, self.layers)
        for components, source, diff_ids in (([image], "sha256:" + "f" * 64, self.layers),
                                             ([image], image.source_image, self.layers[:1]),
                                             (list(reversed(layers)), None, self.layers),
                                             ([image, layers[0]], image.source_image, self.layers)):
            with self.assertRaises(ValueError):
                bind_rafs_layers(components, source, diff_ids)


class ChunkStoreConfigTests(unittest.TestCase):
    RAW = {"endpoint": "https://ucloud.hel1.your-objectstorage.com", "bucket": "ucloud", "region": "hel1",
           "prefix": "/production/chunks/", "access_key_id_env": "HETZNER_S3_ACCESS_KEY_ID",
           "secret_access_key_env": "HETZNER_S3_SECRET_ACCESS_KEY", "force_path_style": False,
           "index_url": "http://10.42.0.2:5090", "index_listen": "10.42.0.2:5090",
           "index_database": "/var/lib/ucloud-chunk-index/index.sqlite",
           "read_token_file": "/var/lib/ucloud-chunk-index/read.token",
           "write_token_file": "/var/lib/ucloud-chunk-index/write.token", "url_ttl_seconds": 86400,
           "mount_granularity": "image", "nydus_image": "/usr/local/bin/nydus-image", "concurrent_misses": 32}

    def test_off_by_default_strict_and_round_trips(self):
        from ucloud_sandboxes.environment_config import EnvironmentDeploymentConfig
        bare = EnvironmentDeploymentConfig.from_dict({"trusted_keys_file": "/keys.json"})
        self.assertIsNone(bare.chunk_store)
        self.assertNotIn("chunk_store", bare.to_dict())  # Older releases still read rendered configs.
        configured = EnvironmentDeploymentConfig.from_dict({"trusted_keys_file": "/keys.json", "chunk_store": self.RAW})
        store = configured.chunk_store
        self.assertEqual((store.endpoint, store.prefix), ("https://hel1.your-objectstorage.com", "production/chunks"))
        self.assertEqual(EnvironmentDeploymentConfig.from_dict(configured.to_dict()), configured)
        for changes in ({"extra": 1}, {"mount_granularity": "file"}, {"url_ttl_seconds": 60},
                        {"access_key_id_env": "not a name"}, {"index_database": "relative"},
                        {"force_path_style": "no"}, {"prefix": "a/../b"}):
            raw = {key: value for key, value in (self.RAW | changes).items() if value is not None}
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                EnvironmentDeploymentConfig.from_dict({"trusted_keys_file": "/keys.json", "chunk_store": raw})
        with self.assertRaises(ValueError):  # Every field is required.
            EnvironmentDeploymentConfig.from_dict({"trusted_keys_file": "/keys.json", "chunk_store": {
                key: value for key, value in self.RAW.items() if key != "nydus_image"}})


if __name__ == "__main__":
    unittest.main()
