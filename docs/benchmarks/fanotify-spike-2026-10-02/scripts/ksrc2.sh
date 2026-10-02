#!/bin/bash
# S13: more kernel source passages (appended to kernel-source-notes.txt).
cd /data/k/linux-source-7.0.0 || exit 1
o=/data/results/kernel-source-notes.txt
{
  echo "### fanotify_user.c: fanotify_events_supported (filesystems opt in to HSM)"
  sed -n '/^static int fanotify_events_supported/,/^}/p' fs/notify/fanotify/fanotify_user.c
  echo "### filesystems that set SB_I_ALLOW_HSM"
  grep -rn "SB_I_ALLOW_HSM" fs/ include/ | grep -v "fs/notify"
  echo "### fanotify_user.c: watchdog"
  sed -n '/^static void perm_group_watchdog(/,/^}/p' fs/notify/fanotify/fanotify_user.c
  echo "### fs/erofs/fileio.c: erofs_fileio_scan_folio (rq merge check vs flat-device translation)"
  sed -n '/^static int erofs_fileio_scan_folio/,/^}/p' fs/erofs/fileio.c
  echo "### fs/erofs/data.c: erofs_init_metabuf / erofs_bread (metadata through the backing file's page cache)"
  sed -n '/^void \*erofs_bread/,/^}/p' fs/erofs/data.c
  sed -n '/^void erofs_init_metabuf/,/^}/p' fs/erofs/data.c
  sed -n '/^int erofs_init_metabuf/,/^}/p' fs/erofs/data.c
  echo "### fs/erofs/ishare.c: erofs_ishare_fill_inode and the fingerprint"
  sed -n '/^bool erofs_ishare_fill_inode/,/^}/p' fs/erofs/ishare.c
  sed -n '/^int erofs_xattr_fill_inode_fingerprint/,/^}/p' fs/erofs/xattr.c
  sed -n '/^static int erofs_ishare_file_open/,/^}/p' fs/erofs/ishare.c
  echo "### fs/erofs/Kconfig: PAGE_CACHE_SHARE"
  sed -n '/config EROFS_FS_PAGE_CACHE_SHARE/,/^config /p' fs/erofs/Kconfig
  echo "### fs/xfs/xfs_reflink.c: remap prep (partial EOF block rule)"
  grep -n "EOF block\|partial\|IS_ALIGNED\|EINVAL" fs/xfs/xfs_reflink.c | head -20
  grep -n "Don't allow\|EOF\|ALIGN" fs/remap_range.c | head -20
} >> $o 2>&1
wc -l $o
