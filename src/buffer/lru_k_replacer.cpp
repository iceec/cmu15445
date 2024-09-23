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

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}


// 所有回合变成均摊logn  而不是一个 o(n)剩下o1
auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {

  // 全局加锁把
  std::lock_guard<std::mutex> lock(latch_); 
  // 先遍历所有计算 最大frame_id
  std::optional<frame_id_t> ans;

  auto entry = node_tree_.begin();

  // 本身不可以被驱逐 然后还是第一个 那么就不行了
  if (entry == node_tree_.end() || entry->first.is_evictable_ == false) return ans;


  frame_id_t evict_id = entry->second;

  // 都进行删除
  node_store_.erase(evict_id);
  node_info_.erase(evict_id);
  node_tree_.erase(entry);
  curr_size_--;
  ans = evict_id;

  return ans;
}
// access type在这一步暂时没有用
void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  // 太大了 不可以的 超过了replace的范围
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) <= replacer_size_, "frame_id is too big");

  // 获取当前时间
  std::lock_guard<std::mutex> lock(latch_); 
  auto it = node_store_.find(frame_id);

  if (it == node_store_.end())  // 之间没进来过的
  {
    node_store_[frame_id] = LRUKNode(frame_id, k_);  // 这里面的K有待考究 看看有没有别的实现
  }

  auto &node = node_store_[frame_id];

  // 头插进入当前的时间点
  node.history_.push_front(current_timestamp_);

  // 一共只需要两个现在你是三个了 那么就清除一个就行
  if (node.history_.size() > k_) {
    node.history_.pop_back();  // 尾部删去一个
  }

  // 找到之前的info
  auto info_it = node_info_.find(frame_id);

  // 之前有的话那么就要删除
  if (info_it != node_info_.end()) {
    auto entry = info_it->second;
    node_info_.erase(frame_id);
    node_tree_.erase(entry);
  }

  // 添加进入
  LRUKNode_Info new_entry{node.is_evictable_, node.history_.size() >= k_, current_timestamp_};

  node_info_[frame_id] = new_entry;
  node_tree_[new_entry] = frame_id;

  current_timestamp_++;  // 主要记录为此就行了
}

void LRUKReplacer::change_info_and_tree(frame_id_t id, bool is_evictable, bool is_has_k_size, size_t time_stamp) {
  auto info_it = node_info_.find(id);

  BUSTUB_ASSERT(info_it != node_info_.end(), "change info tree");

  auto entry = info_it->second;
  LRUKNode_Info new_entry{is_evictable, is_has_k_size, time_stamp};

  // 删去并添加现在的结点 in tree
  node_tree_.erase(entry);

  node_tree_[new_entry] = id;
  info_it->second = new_entry;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  // 先保证 访问大小不能出错
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) <= replacer_size_, "frame_id is too big");

  auto it = node_store_.find(frame_id); //反正是不删除 在哪加都行
  // 这种情况ID也是无效的 考虑assert
  if (it == node_store_.end()) return;
  // 原来 no  现在 yes
  std::lock_guard<std::mutex> lock(latch_); 
  if (set_evictable && it->second.is_evictable_ == false) {
    curr_size_++;
    it->second.is_evictable_ = true;
    change_info_and_tree(frame_id, it->second.is_evictable_, it->second.history_.size() >= k_,
                         it->second.history_.front());
    return;
  }
  // 原来 yes  现在 no
  if (set_evictable == false && it->second.is_evictable_) {
    curr_size_--;
    it->second.is_evictable_ = false;
    change_info_and_tree(frame_id, it->second.is_evictable_, it->second.history_.size() >= k_,
                         it->second.history_.front());
    return;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  // 先保证 访问大小不能出错
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) <= replacer_size_, "frame_id is too big");
  std::lock_guard<std::mutex> lock(latch_); 
  auto it = node_store_.find(frame_id);

  // 没找到或者本身情况是不可驱逐的
  if (it == node_store_.end() || it->second.is_evictable_ == false) return;

  // 移除走了 整个的大小要发生改变
  node_store_.erase(frame_id);
  BUSTUB_ASSERT(node_info_.count(frame_id), "");
  // 都删除就好了
  auto entry = node_info_[frame_id];
  node_tree_.erase(entry);
  node_info_.erase(frame_id);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
