#include <cassert>
#include <cstdint>
#include <cstdio>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"
/*
思路捋一下
首先一个header会指向多个目录，一个目录会指向多个桶
header通过key的前几位映射到目录，目录通过后几位的key映射到桶
要实现的插入和移除都应该逐步hash到目标桶
*/
namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  // std::cout << header_max_depth << directory_max_depth << bucket_max_size << "\n";
  index_name_ = name;
  // Create a new header page
  page_id_t page_id;
  auto tmp_header_guard = bpm_->NewPageGuarded(&page_id);
  auto header_guard = tmp_header_guard.UpgradeWrite();
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  header_page->Init(header_max_depth);
  header_page_id_ = page_id;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
// 在哈希表中查找与给定键相关联的值
// key:要查找的键;result:与给定键相关联的值; transaction 当前事务
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  // 获取header page
  ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<ExtendibleHTableHeaderPage>();
  // 通过hash值获取dir_page_id。若dir_page_id为非法id则未找到
  auto hash = Hash(key);
  auto dirIndex = header_page->HashToDirectoryIndex(hash);
  page_id_t directory_page_id = header_page->GetDirectoryPageId(dirIndex);
  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }
  // 获取dir_page
  header_guard.Drop();
  ReadPageGuard directory_guard = bpm_->FetchPageRead(directory_page_id);
  auto directory_page = directory_guard.As<ExtendibleHTableDirectoryPage>();
  // 通过hash值获取bucket_page_id。若bucket_page_id为非法id则未找到
  auto bucket_index = directory_page->HashToBucketIndex(hash);
  auto bucket_page_id = directory_page->GetBucketPageId(bucket_index);
  LOG_DEBUG("要获得的bucket_page_id是：%d,系数是%d", bucket_page_id, hash);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  ReadPageGuard bucket_guard = bpm_->FetchPageRead(bucket_page_id);
  // 获取bucket_page
  directory_guard.Drop();
  auto bucket_page = bucket_guard.As<ExtendibleHTableBucketPage<K, V, KC>>();
  // 在bucket_page上查找
  V value;
  if (bucket_page->Lookup(key, value, cmp_)) {
    result->push_back(value);
    return true;
  }
  return false;
}



/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  std::vector<V> valuesFound;
  bool keyExists = GetValue(key, &valuesFound, transaction);
  if (keyExists) {
    // 已存在直接返回false表示不插入重复键
    return false;
  }
  auto hash_key = Hash(key);
  // 获取header page
  WritePageGuard header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  // 使用header_page来获取目录索引
  auto directory_index = header_page->HashToDirectoryIndex(hash_key);
  //用目录索引获取目录页，然后找到頁的目录ID
  page_id_t directory_page_id = header_page->GetDirectoryPageId(directory_index);
  // 若dir_page_id为非法id则在新的dir_page添加
  if (directory_page_id == INVALID_PAGE_ID) {
    return InsertToNewDirectory(header_page, directory_index, hash_key, key, value);
  }
  // 对directory加锁
  header_guard.Drop();
  WritePageGuard directory_guard = bpm_->FetchPageWrite(directory_page_id);
  // 获取 dir page
  auto directory_page = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
  // 通过hash值获取bucket_page_id。若bucket_page_id为非法id则在新的bucket_page添加
  auto bucket_index = directory_page->HashToBucketIndex(hash_key);
  auto bucket_page_id = directory_page->GetBucketPageId(bucket_index);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return InsertToNewBucket(directory_page, bucket_index, key, value);
  }

  // 对bucket加锁
  WritePageGuard bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  // LOG_DEBUG("要插入的bucket是：%d，哈希数是%d",bucket_page_id,hash_key);
  // 获取bucket_page插入元素，如果插入失败则代表该bucket_page满了
  if (bucket_page->Insert(key, value, cmp_)) {
    LOG_DEBUG("Insert bucket %d Success!", bucket_page_id);
    return true;
  }
  auto h = 1U << directory_page->GetGlobalDepth();
  // 判断是否能添加度，不能则返回
  if (directory_page->GetLocalDepth(bucket_index) == directory_page->GetGlobalDepth()) {
    if (directory_page->GetGlobalDepth() >= directory_page->GetMaxDepth()) {
      return false;
    }
    directory_page->IncrGlobalDepth();
    // 需要更新目录页
    for (uint32_t i = h; i < (1U << directory_page->GetGlobalDepth()); ++i) {
      auto new_bucket_page_id = directory_page->GetBucketPageId(i - h);
      auto new_local_depth = directory_page->GetLocalDepth(i - h);
      directory_page->SetBucketPageId(i, new_bucket_page_id);
      directory_page->SetLocalDepth(i, new_local_depth);
    }
  }
  directory_page->IncrLocalDepth(bucket_index);
  directory_page->IncrLocalDepth(bucket_index + h);
  // 拆份bucket
  if (!SplitBucket(directory_page, bucket_page, bucket_index)) {
    return false;
  }
  bucket_guard.Drop();
  directory_guard.Drop();
  return Insert(key, value, transaction);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  page_id_t directory_page_id = INVALID_PAGE_ID;
  WritePageGuard directory_guard = bpm_->NewPageGuarded(&directory_page_id).UpgradeWrite();
  auto directory_page = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
  directory_page->Init(directory_max_depth_);
  header->SetDirectoryPageId(directory_idx, directory_page_id);
  uint32_t bucket_idx = directory_page->HashToBucketIndex(hash);
  LOG_DEBUG("InsertToNewDirectory directory_page_id:%d", directory_page_id);
  return InsertToNewBucket(directory_page, bucket_idx, key, value);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  page_id_t bucket_page_id = INVALID_PAGE_ID;
  WritePageGuard bucket_guard = bpm_->NewPageGuarded(&bucket_page_id).UpgradeWrite();
  auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  bucket_page->Init(bucket_max_size_);
  directory->SetBucketPageId(bucket_idx, bucket_page_id);
  LOG_DEBUG("InsertToNewBucket bucket_page_id:%d", bucket_page_id);
  return bucket_page->Insert(key, value, cmp_);
}


/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  for (uint32_t i = 0; i < (1U << directory->GetGlobalDepth()); ++i) {
    // 检查目录条目是否需要更新为指向新桶
    // 如果目录项对应的是原桶
    if (directory->GetBucketPageId(i) == directory->GetBucketPageId(new_bucket_idx)) {
      if (i & local_depth_mask) {
        // 如果这个目录项的在新局部深度位上的值为1，应该指向新桶
        directory->SetBucketPageId(i, new_bucket_page_id);
        directory->SetLocalDepth(i, new_local_depth);
      } else {
        // 否则，它仍然指向原桶，但其局部深度需要更新
        directory->SetLocalDepth(i, new_local_depth);
      }
    }
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
  一个找了很久的bug：Buckets can only be merged with their split image if their split image has the same local depth.
  merge时需要调用DeletePage()吗？
  对于LD为0的情况如何处理对我来说是个难点，感觉我不太擅长处理这种边界条件。
 */
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  auto header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<ExtendibleHTableHeaderPage>();
  uint32_t hash = Hash(key);  // for test
  auto directory_index = header_page->HashToDirectoryIndex(hash);
  auto directory_page_id = header_page->GetDirectoryPageId(directory_index);
  if (static_cast<int>(directory_page_id) == INVALID_PAGE_ID) {
    return false;
  }

  WritePageGuard directory_guard = bpm_->FetchPageWrite(directory_page_id);
  auto directory_page = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
  auto bucket_index = directory_page->HashToBucketIndex(hash);
  auto bucket_page_id = directory_page->GetBucketPageId(bucket_index);
  if (static_cast<int>(bucket_page_id) == INVALID_PAGE_ID) {
    return false;
  }

  // find the target key in the third level
  WritePageGuard bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  bool remove_success = bucket_page->Remove(key, cmp_);
  if (!remove_success) {
    return false;
  }

  while (bucket_page->IsEmpty()) {
    bucket_guard.Drop();
    auto bucket_local_depth = directory_page->GetLocalDepth(bucket_index);
    if (bucket_local_depth == 0) {
      break;
    }
    //得到分裂的桶，准备重新合并
    auto merge_bucket_index = directory_page->GetSplitImageIndex(bucket_index);
    auto merge_bucket_local_depth = directory_page->GetLocalDepth(merge_bucket_index);
    auto merge_bucket_page_id = directory_page->GetBucketPageId(merge_bucket_index);

    if (bucket_local_depth == merge_bucket_local_depth) {  //空桶和想要合并桶深度一样
      uint32_t traverse_bucket_idx =
          std::min(bucket_index & directory_page->GetLocalDepthMask(bucket_index), merge_bucket_index);
      uint32_t distance = 1 << (bucket_local_depth -
                                1);  //减小局部深度合并在一起比如 011应该和111合并在一起所以是bucket_local_depth - 1
      uint32_t new_local_depth = bucket_local_depth - 1;
      for (uint32_t i = traverse_bucket_idx; i < directory_page->Size(); i += distance) {
        directory_page->SetBucketPageId(i, merge_bucket_page_id);
        directory_page->SetLocalDepth(i, new_local_depth);
      }

      if (new_local_depth == 0) {
        break;
      }
      //假设之前是 011满了找到了111
      auto split_image_bucket_index = directory_page->GetSplitImageIndex(merge_bucket_index);  // 11->01
      auto split_image_bucket_page_id = directory_page->GetBucketPageId(split_image_bucket_index);
      WritePageGuard split_image_bucket_guard = bpm_->FetchPageWrite(split_image_bucket_page_id);
      if (split_image_bucket_page_id == INVALID_PAGE_ID) {
        break;
      }
      auto helper = bucket_page_id;
      // 我当时是咋想的？写出下面这一行抽象代码……
      // gradescope不提供测试源码，我这种复现不了测试的菜鸡重新review了一遍代码才找出这个bug。
      // directory_page->SetBucketPageId(bucket_index, 0);
      bucket_index = split_image_bucket_index;
      bucket_page_id = split_image_bucket_page_id;
      bucket_guard = std::move(split_image_bucket_guard);
      bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
      bpm_->DeletePage(helper);
    } else {
      // Can not merge because of (LD != LD(split_image))
      break;
    }
    while (directory_page->CanShrink()) {
      directory_page->DecrGlobalDepth();
    }
  }

  while (directory_page->CanShrink()) {
    directory_page->DecrGlobalDepth();
  }

  return remove_success;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::SplitBucket(ExtendibleHTableDirectoryPage *directory,
                                                    ExtendibleHTableBucketPage<K, V, KC> *bucket, uint32_t bucket_idx)
    -> bool {
  // 创建新bucket_page
  page_id_t split_page_id;
  WritePageGuard split_bucket_guard = bpm_->NewPageGuarded(&split_page_id).UpgradeWrite();
  if (split_page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto split_bucket = split_bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  split_bucket->Init(bucket_max_size_);
  uint32_t split_idx = directory->GetSplitImageIndex(bucket_idx);
  uint32_t local_depth = directory->GetLocalDepth(bucket_idx);
  directory->SetBucketPageId(split_idx, split_page_id);
  directory->SetLocalDepth(split_idx, local_depth);
  LOG_DEBUG("Spilt bucket_page_id:%d", split_page_id);
  // 将原来满的bucket_page拆分到两个page页中
  page_id_t bucket_page_id = directory->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }

  // 先将原来的数据取出，放置在entries容器中
  int size = bucket->Size();
  std::list<std::pair<K, V>> entries;
  for (int i = 0; i < size; i++) {
    entries.emplace_back(bucket->EntryAt(i));
  }
  // 清空bucket:size_ = 0
  bucket->Clear();

  // 分到两个bucket_page中
  for (const auto &entry : entries) {
    uint32_t target_idx = directory->HashToBucketIndex(Hash(entry.first));
    page_id_t target_page_id = directory->GetBucketPageId(target_idx);
    if (target_page_id == bucket_page_id) {
      bucket->Insert(entry.first, entry.second, cmp_);
    } else if (target_page_id == split_page_id) {
      split_bucket->Insert(entry.first, entry.second, cmp_);
    }
  }
  return true;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub

/* 服务器端跑test，无源码，p1还没啥感觉，p2真是被折磨到了……(记录一下比较印象深刻的bug)
  BUG1: NewPage()后的没有Update取读写锁，这个bug的原因主要是写代码时对pageguard部分理解不够深刻。
  BUG2: 对page_id的初始化，应该初始化为INVALID_PAGE_ID。其实我一开始想过初始化的问题，但想当然地认为初始化为0就行，
        这样不用耗费时间来初始化。对于大多数情况下确实没问题，但在多个table时会有逻辑漏洞，要是在实际场景中情况将会更加
        复杂。代码应该是严谨的，有时候直觉犯下的小错误会make you crazy，因此在模棱两可的地方要多加思考。
  收获: 上述的bug2应该是折磨我时间最长的一个bug了，一个“靠感觉”写下的代码将会漏洞百出，谨记——严谨，意识到有地方可能出现
        bug就去思考、完善，不要带着侥幸心理。
        assert()会make sense，它一定程度上能帮助coder更好地定位bug实际位置。
        对于抽象层的把握不足，好的抽象、函数接口能帮助程序员更好地理解代码逻辑。
 */