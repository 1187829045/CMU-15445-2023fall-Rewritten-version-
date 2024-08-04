//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"
#include "common/logger.h"

namespace bustub {
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}
//驱逐可驱逐frams中的向后 k-距离最大frames
auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  //std::cout<<"-------------------------------------------this is Evict"<<std::endl;
  std::lock_guard<std::mutex> lock(latch_);
  if(curr_size_==0){
    return false;
  }
  LRUKNode *evict= nullptr;
  //遍历node_store找到最向后的驱逐掉
  bool is_find=false;
  bool have_is_inf=false;
  size_t max_time=0;
  size_t inf_max_time=0;
  for(auto &pair : node_store_){
    auto &page = pair.second;
    //判断是不是可以移除
    if(!*page.Get_is_evictable_()){
      //std::cout<<"------------------------不可移除的页=="<<pair.first<<std::endl;
      continue;
    }
    std::list<size_t> *page_history = page.Get_history();
    //如果没有访问记录直接驱逐
    if(page_history->empty()){
      is_find =true;
      evict = &page;
      *frame_id=pair.first;
      break;
    }
   // std::cout<<"-----------------------------------------------------"<<std::endl;
    //可以移除就要判断历史有没有小于k，并获取最近访问时间
//    std::cout<<"-------------------------------------------------page_history->size()="<<page_history->size()<<std::endl;
//    std::cout<<"-------------------------------------------------k_="<<k_<<std::endl;
    if((size_t)page_history->size()<k_){
      have_is_inf=true;
     if (inf_max_time<current_timestamp_ - page_history->front()){
       is_find =true;
       evict = &page;
       *frame_id=pair.first;
       inf_max_time=current_timestamp_ - page_history->front();
//       std::cout<<"--------------------------------------------------------"<<std::endl;
     }
    }else if(!have_is_inf&&(size_t)page_history->size()==k_){
      if (max_time<current_timestamp_ - page_history->front()){
        is_find =true;
        evict = &page;
        *frame_id=pair.first;
        max_time=current_timestamp_ - page_history->front();
//        std::cout<<"--------------------------------------------------------"<<std::endl;
      }
    }
  }
  if(is_find){
    curr_size_--;
    evict->Get_history()->clear();
    node_store_.erase(*frame_id);
    return true;
  }
  return false;
}
//记录给定frames ID 在当前时间戳的访问。这个方法应在一个页面在缓冲池管理器中被固定之后调用。
void LRUKReplacer::RecordAccess(frame_id_t frame_id,AccessType access_type) {
  //std::cout<<"-------------------------------------------this is RecordAccess"<<std::endl;
  //std::cout<<"-------------------------------------------frame_id="<<frame_id<<std::endl;
  std::lock_guard<std::mutex> lock(latch_);
  auto helper = static_cast<size_t>(frame_id);
  BUSTUB_ASSERT(helper <= replacer_size_, "invalid frame_id");

  auto iter = node_store_.find(frame_id);
  if (iter == node_store_.end()) {
    auto new_page_ptr = std::make_unique<LRUKNode>();
    if (access_type != AccessType::Scan) {
      new_page_ptr->Get_history()->push_back(current_timestamp_++);
    }
    node_store_.insert(std::make_pair(frame_id, *new_page_ptr));
    //debug代码
   // auto itr = node_store_.find(frame_id);
    //std::cout<<"-------------------------------------------itr->first="<<itr->first<<std::endl;
  } else {
    auto &node = iter->second;
    if (access_type != AccessType::Scan) {
      if (node.Get_history()->size() == k_) {
        node.Get_history()->pop_front();
      }
      node.Get_history()->push_back(current_timestamp_++);
      //auto itr = node_store_.find(frame_id);
      //std::cout<<"-------------------------------------------itr->first="<<itr->first<<std::endl;
      //std::cout<<"------------------------------------------- node.Get_history()->front()"<< node.Get_history()->front()<<std::endl;
    }
  }
}
//此方法控制frames是否可驱逐。它还控制 LRUKReplacer 的大小，当页面的固定计数达到 0 时，其相应的frames被标记为可驱逐，替换器的大小增加。
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
//  std::cout<<"-------------------------------------------this is SetEvictable"<<std::endl;
  std::scoped_lock<std::mutex> lock(latch_);
  auto helper = static_cast<size_t>(frame_id);
  BUSTUB_ASSERT(helper <= replacer_size_, "invalid frame_id");

  std::unique_ptr<LRUKNode> new_page_ptr;
  auto iter = node_store_.find(frame_id);
  if (iter == node_store_.end()) {
    // Fail to find out the LRUKNode.
    new_page_ptr = std::make_unique<LRUKNode>();
    node_store_.insert(std::make_pair(frame_id, *new_page_ptr));
  }
  auto &node = (iter == node_store_.end()) ? *new_page_ptr : iter->second;
  if (set_evictable && !*node.Get_is_evictable_()) {
    *node.Get_is_evictable_() = set_evictable;
    curr_size_++;
  }
  if (!set_evictable && *node.Get_is_evictable_()) {
    *node.Get_is_evictable_() = set_evictable;
    curr_size_--;
  }
}
//清除与frames相关的所有访问历史记录。这个方法只应在缓冲池管理器中删除页面时调用。
void LRUKReplacer::Remove(frame_id_t frame_id) {
//  std::cout<<"-------------------------------------------this is Remove"<<std::endl;
  std::scoped_lock<std::mutex> lock(latch_);
  auto iter = node_store_.find(frame_id);
  if (iter == node_store_.end()) {
    return;
  }
  auto &node = iter->second;
  BUSTUB_ASSERT(*node.Get_is_evictable_(), "Called on a non-evictable frame.");

  node.Get_history()->clear();
  node_store_.erase(frame_id);
  curr_size_--;
}
//此方法返回当前在 LRUKReplacer 中的可驱逐frames的数量。
auto LRUKReplacer::Size() -> size_t {
  //std::cout<<"-------------------------------------------this is Size"<<std::endl;
  return curr_size_;
}

}  // namespace bustub
