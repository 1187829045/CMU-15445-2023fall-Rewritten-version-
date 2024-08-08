//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.h
//
// Identification: src/include/container/disk/hash/extendible_hash_table.h
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <deque>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "concurrency/transaction.h"
#include "container/hash/hash_function.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

/**
 * Implementation of extendible hash table that is backed by a buffer pool
 * manager. Non-unique keys are supported. Supports insert and delete. The
 * table grows/shrinks dynamically as buckets become full/empty.
 */
template <typename K, typename V, typename KC>
class DiskExtendibleHashTable {
 public:
  /**
   * @brief Creates a new DiskExtendibleHashTable.
   *
   * @param name
   * @param bpm 使用的缓冲池管理器
   * @param cmp 键比较器
   * @param hash_fn 哈希函数
   * @param header_max_depth 允许的标题页的最大深度
   * @param directory_max_depth 允许的目录页的最大深度
   * @param bucket_max_size 允许的 bucket 页数组的最大大小
   */
  explicit DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm, const KC &cmp,
                                   const HashFunction<K> &hash_fn, uint32_t header_max_depth = HTABLE_HEADER_MAX_DEPTH,
                                   uint32_t directory_max_depth = HTABLE_DIRECTORY_MAX_DEPTH,
                                   uint32_t bucket_max_size = HTableBucketArraySize(sizeof(std::pair<K, V>)));

  /** TODO(P2): Add implementation
* 将键值对插入哈希表。
*
* @param key 要创建的键
* @param value 与键关联的值
* @param transaction 当前事务
* @return 如果插入成功则返回 true，否则返回 false
   */
  auto Insert(const K &key, const V &value, Transaction *transaction = nullptr) -> bool;

  /** TODO(P2): Add implementation
* 将键值对插入哈希表。
*
* @param key 要创建的键
* @param value 与键关联的值
* @param transaction 当前事务
* @return 如果插入成功则返回 true，否则返回 false
   */
  auto Remove(const K &key, Transaction *transaction = nullptr) -> bool;

      /** TODO(P2): Add implementation
   * Get the value associated with a given key in the hash table.
   *
   * Note(fall2023): This semester you will only need to support unique key-value pairs.
   *
   * @param key the key to look up
   * @param[out] result the value(s) associated with a given key
   * @param transaction the current transaction
   * @return the value(s) associated with the given key
   */
  auto GetValue(const K &key, std::vector<V> *result, Transaction *transaction = nullptr) const -> bool;

  /**
   * Helper function to verify the integrity of the extendible hash table's directory.
   */
  void VerifyIntegrity() const;

  /**
   * Helper function to expose the header page id.
   */
  auto GetHeaderPageId() const -> page_id_t;

  /**
   * Helper function to print out the HashTable.
   */
  void PrintHT() const;
  auto SplitBucket(ExtendibleHTableDirectoryPage *directory,ExtendibleHTableBucketPage<K, V, KC> *bucket, uint32_t bucket_idx)-> bool;

 private:
  /**
   * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
   * for extendible hashing.
   *
   * @param key the key to hash
   * @return the down-casted 32-bit hash
   */
  auto Hash(K key) const -> uint32_t;

  auto InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx, uint32_t hash, const K &key,
                            const V &value) -> bool;

  auto InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx, const K &key, const V &value)
      -> bool;

  void  UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory, uint32_t new_bucket_idx,
                              page_id_t new_bucket_page_id, uint32_t new_local_depth, uint32_t local_depth_mask);

  void MigrateEntries(ExtendibleHTableBucketPage<K, V, KC> *old_bucket,
                      ExtendibleHTableBucketPage<K, V, KC> *new_bucket, uint32_t new_bucket_idx,
                      uint32_t local_depth_mask);

  // member variables
  //哈希表的名称
  std::string index_name_;
  BufferPoolManager *bpm_;
  KC cmp_;
  HashFunction<K> hash_fn_;
  uint32_t header_max_depth_;
  uint32_t directory_max_depth_;
  uint32_t bucket_max_size_;
  page_id_t header_page_id_;
};

}  // namespace bustub
