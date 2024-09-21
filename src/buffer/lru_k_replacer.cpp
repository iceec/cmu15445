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

auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  // 先遍历所有计算 最大frame_id
  std::optional<frame_id_t> ans;
  bool has_k_dis;
  size_t dis;
  for (auto it = node_store_.begin(); it != node_store_.end(); ++it) {
    if (it->second.is_evictable_)  // 可以被驱逐
    {
      
      // 当前没有选项呢
      if (!ans.has_value()) {
        ans = it->first;
        has_k_dis = it->second.history_.size() == k_ ? true : false;
        dis = it->second.history_.front();  // 最后排除去的是dis最小的
      }
      // 现在的情况是之前有值 不用比较 一定不是我
      else if (has_k_dis == false && it->second.history_.size() == k_)
        continue;
      // 不用进行比较了 一定是我
      else if (has_k_dis == true && it->second.history_.size() < k_) {
        ans = it->first;
        has_k_dis = false;
        dis = it->second.history_.front();
      }
      // 需要进行比较 那么就是不改变 bool 只改变dis
      else if (it->second.history_.front() < dis) {
        dis = it->second.history_.front();
        ans = it->first;
      }
    }
  }

  if (ans.has_value()) {
    auto id = ans.value();
    node_store_.erase(id);  // 删除历史
    curr_size_--;
  }

  return ans;
}
// access type在这一步暂时没有用
void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  // 太大了 不可以的 超过了replace的范围
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) <= replacer_size_, "frame_id is too big");

  // 获取当前时间
  
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

  current_timestamp_++; // 主要记录为此就行了
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  // 先保证 访问大小不能出错
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) <= replacer_size_, "frame_id is too big");

  auto it = node_store_.find(frame_id);
  // 这种情况ID也是无效的 考虑assert
  if (it == node_store_.end()) return;
  // 原来 no  现在 yes
  if (set_evictable && it->second.is_evictable_ == false) {
    curr_size_++;
    it->second.is_evictable_ = true;
    return;
  }
  // 原来 yes  现在 no
  if (set_evictable == false && it->second.is_evictable_) {
    curr_size_--;
    it->second.is_evictable_ = false;
    return;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  // 先保证 访问大小不能出错
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) <= replacer_size_, "frame_id is too big");

  auto it = node_store_.find(frame_id);

  // 没找到或者本身情况是不可驱逐的
  if (it == node_store_.end() || it->second.is_evictable_ == false) return;

  // 移除走了 整个的大小要发生改变 
  node_store_.erase(frame_id);
  curr_size_--; 
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
