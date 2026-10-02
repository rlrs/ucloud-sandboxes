#!/bin/bash
# S13: extract the kernel source passages the README cites (7.0.0-30, Ubuntu-patched 7.0.12).
cd /data/k/linux-source-7.0.0 || exit 1
o=/data/results/kernel-source-notes.txt
{
  echo "### fs/notify/fanotify/fanotify_user.c: process_access_response"
  sed -n '/^static int process_access_response/,/^}/p' fs/notify/fanotify/fanotify_user.c
  echo "### fs/notify/fanotify/fanotify_user.c: fanotify_release"
  sed -n '/^static int fanotify_release/,/^}/p' fs/notify/fanotify/fanotify_user.c
  echo "### fs/notify/fanotify/fanotify.c: fanotify_get_response"
  sed -n '/^static int fanotify_get_response/,/^}/p' fs/notify/fanotify/fanotify.c
  echo "### fanotify_user.c: pre-content mark checks"
  grep -n "PRE_CONTENT\|pre_content\|FSNOTIFY_PRIO\|S_ISREG\|d_is_reg\|EOPNOTSUPP\|-EINVAL" fs/notify/fanotify/fanotify_user.c | head -60
  echo "### fs/notify/fsnotify.c: open-time mode"
  sed -n '/^int fsnotify_open_perm_and_set_mode/,/^}/p' fs/notify/fsnotify.c
  echo "### include/linux/fsnotify.h: area perm / mmap / pre-content hooks"
  sed -n '/static inline int fsnotify_file_area_perm/,/^}/p' include/linux/fsnotify.h
  sed -n '/static inline int fsnotify_mmap_perm/,/^}/p' include/linux/fsnotify.h
  sed -n '/static inline int fsnotify_pre_content/,/^}/p' include/linux/fsnotify.h
  grep -n "FMODE_FSNOTIFY\|FMODE_NONOTIFY" include/linux/fs.h | head
  echo "### mm: fsnotify hooks"
  grep -rn "fsnotify_mmap_perm\|fsnotify_file_area_perm\|filemap_fsnotify\|fsnotify_filemap" mm/*.c | head -20
  echo "### fs/read_write.c: rw_verify_area and vfs_iocb_iter_read"
  sed -n '/^int rw_verify_area/,/^}/p' fs/read_write.c
  sed -n '/^ssize_t vfs_iocb_iter_read/,/^}/p' fs/read_write.c
  echo "### fs/erofs/fileio.c"
  sed -n '/^static void erofs_fileio_ki_complete/,/^}/p' fs/erofs/fileio.c
  sed -n '/^static void erofs_fileio_rq_submit/,/^}/p' fs/erofs/fileio.c
  echo "### fs/erofs: metadata reads in file-backed mode"
  grep -n "f_mapping\|i_mapping\|read_mapping_folio\|fileio\|dif0.file\|filp_open\|directio\|DIRECT_IO\|flatdev" fs/erofs/data.c fs/erofs/super.c fs/erofs/internal.h | head -50
  sed -n '/^int erofs_map_dev/,/^}/p' fs/erofs/data.c
  echo "### fs/erofs/ishare.c"
  grep -n "fileio\|file-backed\|domain_id\|xxh32\|prefix\|fingerprint\|ishare_xattr\|i_size\|return -\|erofs_err\|erofs_info" fs/erofs/ishare.c | head -60
  grep -n "ishare\|INODE_SHARE\|inode_share" fs/erofs/super.c fs/erofs/xattr.c fs/erofs/inode.c fs/erofs/internal.h fs/erofs/erofs_fs.h | head -60
  echo "### fs/xfs reflink page cache handling"
  grep -n "truncate_pagecache_range\|xfs_flush_unmap_range\|filemap_write_and_wait" fs/xfs/xfs_reflink.c fs/xfs/xfs_file.c | head
} > $o 2>&1
wc -l $o
