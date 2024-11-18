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
  std::lock_guard<std::mutex> lock(latch_);

  frame_id_t evict_id;
  bool has{false};
  size_t evict_time{0};
  size_t evict_bkd{std::numeric_limits<size_t>::max()};  // 最大backward k-distance

  for (auto it : node_store_) {
    if (!it.second.is_evictable_) {
      continue;
    }

    auto &node = it.second;
    auto node_size = node.history_.size();
    // 计算 backward k-distance
    size_t backward_k_distance =
        (node_size < k_) ? std::numeric_limits<size_t>::max() : (current_timestamp_ - node.history_.back());

    if (!has || backward_k_distance > evict_bkd ||
        (backward_k_distance == evict_bkd && node.history_.back() < evict_time)) {
      evict_time = node.history_.back();
      evict_bkd = backward_k_distance;
      evict_id = it.first;
      has = true;
    }
  }

  if (has) {
    node_store_.erase(evict_id);
    curr_size_--;
    return evict_id;
  }

  return std::nullopt;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  BUSTUB_ASSERT(frame_id <= static_cast<frame_id_t>(replacer_size_), "frame_id lager than replacer_size");
  std::lock_guard<std::mutex> lock(latch_);

  auto it = node_store_.find(frame_id);

  if (it == node_store_.end()) {
    node_store_[frame_id] = LRUKNode();
    it = node_store_.find(frame_id);
  }

  auto &lists = it->second.history_;

  lists.push_front(current_timestamp_++);

  if (lists.size() > k_) {
    lists.pop_back();
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);

  auto it = node_store_.find(frame_id);

  if (it == node_store_.end()) {
    return;
  }

  bool pre_status = it->second.is_evictable_;
  bool now_status = set_evictable;
  it->second.is_evictable_ = set_evictable;

  if (pre_status && !now_status) {
    curr_size_--;
  } else if (!pre_status && now_status) {
    curr_size_++;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);

  auto it = node_store_.find(frame_id);

  if (it == node_store_.end()) {
    return;
  }
  BUSTUB_ASSERT(it->second.is_evictable_, "remove nonevictable frame");
  node_store_.erase(it);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
