//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }
//找到一个空闲的frame，新分配一个物理页，并将该物理页的内容读取到刚找到的这个frame中
// 在缓冲池中创建一个新页面
auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t replacement_frame_id;
  if (!free_list_.empty()) {
    replacement_frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Evict(&replacement_frame_id)) {
      page_id = nullptr;
      return nullptr;
    }
    auto &helper = pages_[replacement_frame_id];
    if (helper.IsDirty()) {
      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      disk_scheduler_->Schedule({true, helper.GetData(), helper.page_id_, std::move(promise)});
      future.get();
    }
    page_table_.erase(helper.page_id_);
  }
  auto new_page_id = AllocatePage();
  *page_id = new_page_id;
  pages_[replacement_frame_id].ResetMemory();
  pages_[replacement_frame_id].pin_count_ = 0;
  pages_[replacement_frame_id].is_dirty_ = false;
  page_table_.insert(std::make_pair(new_page_id, replacement_frame_id));
  replacer_->SetEvictable(replacement_frame_id, false);
  replacer_->RecordAccess(replacement_frame_id);
  pages_[replacement_frame_id].pin_count_++;
  pages_[replacement_frame_id].page_id_ = new_page_id;
  return &pages_[replacement_frame_id];
}
//给定物理页id，获取该物理页所对应的物理页
auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard lock(latch_);
  if (page_id == INVALID_PAGE_ID) {
    return nullptr;
  }
  if (page_table_.find(page_id) != page_table_.end()) {
    // ! get page
    auto frame_id = page_table_[page_id];
    auto page = pages_ + frame_id;
    // 更新replacer
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    // ! update pin count
    page->pin_count_ += 1;
    return page;
  }
  // Newpage 方法里的
  Page *page;
  frame_id_t frame_id = -1;
  if (!free_list_.empty()) {
    frame_id = free_list_.back();
    free_list_.pop_back();
  } else {
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }
  }
  page = pages_ + frame_id;
  if (page->IsDirty()) {
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({true, page->GetData(), page->GetPageId(), std::move(promise)});
    future.get();
    page->is_dirty_ = false;
  }
  page_table_.erase(page->GetPageId());
  page_table_.emplace(page_id, frame_id);
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  page->ResetMemory();
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  // 从磁盘中读页，读完后写回（之前的是写完后写回）
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({false, page->GetData(), page->GetPageId(), std::move(promise)});
  future.get();
  return page;
}

//将指定的物理页对应的frame的pin_count减1
auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard lock(latch_);

  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  auto frame_id = page_table_[page_id];
  auto page = pages_ + frame_id;
  // 设置脏位,如果原本是脏的或传进的is_dirty是脏的，最终就是脏的
  page->is_dirty_ = is_dirty||page->is_dirty_;
  // if pin count is 0
  if (page->GetPinCount() == 0) {
    return false;
  }
  // pin要-1
  page->pin_count_ -= 1;
  // 如果-1后为0,调用lru-k中的SetEvictable方法，把帧设为可驱逐的
  if (page->GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

//将给定的缓存页写回磁盘
auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard lock(latch_);
  //无效或者不存在
  if (page_id == INVALID_PAGE_ID||page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  // 获得page_id在缓冲池中的位置
  auto page = pages_ + page_table_[page_id];
  // 写回，这里creatpromise方法返回了一个std::promise对象
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({true, page->GetData(), page->GetPageId(), std::move(promise)});
  future.get();
  // 赃位恢复
  page->is_dirty_ = false;
  return true;
}
//将所有有效的缓存页写回磁盘

void BufferPoolManager::FlushAllPages() {
  std::lock_guard lock(latch_);
  for (size_t current_size = 0; current_size < pool_size_; current_size++) {
    // 获得page_id在缓冲池中的位置
    auto page = pages_ + current_size;
    if (page->GetPageId() == INVALID_PAGE_ID) {
      continue;
    }
    // 和flush方法一样
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({true, page->GetData(), page->GetPageId(), std::move(promise)});
    future.get();
    page->is_dirty_ = false;
  }
}

//给定物理页id，将物理页对应的内存页从buffer中删除
auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard lock (latch_);
  if (page_id == INVALID_PAGE_ID) {
    return true;
  }
  // 如果页面存在
  if (page_table_.find(page_id) != page_table_.end()) {
    auto frame_id = page_table_[page_id];
    auto page = pages_ + frame_id;
    // 如果页面用着呢
    if (page->GetPinCount() > 0) {
      return false;
    }
    // 删除页面
    page_table_.erase(page_id);
    free_list_.push_back(frame_id);
    replacer_->Remove(frame_id);
    // 把内存该清的清，page的参数该换的换
    page->ResetMemory();
    page->page_id_ = INVALID_PAGE_ID;
    page->is_dirty_ = false;
    page->pin_count_ = 0;
  }
  // 注释里要求的：调用DeallocatePage()来模仿在磁盘上释放页面。
  DeallocatePage(page_id);
  return true;
}


auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  auto page = FetchPage(page_id);
  return {this, page};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto page = FetchPage(page_id);
  if (page != nullptr) {
    page->RLatch();
  }
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto pg_ptr = FetchPage(page_id);
  pg_ptr->WLatch();
  assert(pg_ptr != nullptr);
  return {this, pg_ptr};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  auto pg_ptr = NewPage(page_id);
  assert(pg_ptr != nullptr);
  return {this, pg_ptr};
}
}  // namespace bustub
