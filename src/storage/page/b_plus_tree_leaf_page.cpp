//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetMaxSize(max_size);
  SetSize(0);
  SetPageType(IndexPageType::LEAF_PAGE);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index" (a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "leaf page out of range");
  return key_array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "value at leaf");
  return rid_array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::FindMatchValue(const KeyType &key,
                                                const KeyComparator &Com) const -> std::optional<ValueType> {
  if (GetSize() == 0) {
    return std::nullopt;
  }
  int l = 0, r = GetSize() - 1;
  int mid = l;
  while (l <= r) {
    mid = (l + r) / 2;
    auto tmp = Com(key_array_[mid], key);
    if (tmp == 0) {
      return rid_array_[mid];
    } else if (tmp > 0) {
      r = mid - 1;
    } else {
      l = mid + 1;
    }
  }
  return std::nullopt;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &Com) -> bool {
  BUSTUB_ASSERT(GetSize() < GetMaxSize(), "leaf insert");
  auto pos_option = LowerBound(key, Com);
  int pos = GetSize();
  // uppper bound的寻找 >= 的
  if (pos_option) {
    pos = pos_option.value();
    if (Com(key, key_array_[pos]) == 0) {
      return false;
    }
  }
  // copy
  std::copy_backward(key_array_ + pos, key_array_ + GetSize(), key_array_ + GetSize() + 1);
  std::copy_backward(rid_array_ + pos, rid_array_ + GetSize(), rid_array_ + GetSize() + 1);
  // insert
  key_array_[pos] = key;
  rid_array_[pos] = value;
  ChangeSizeBy(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::FullInsert(const KeyType &key, const ValueType &value, const KeyComparator &Com,
                                            BPlusTreeLeafPage *other_page,
                                            page_id_t other_page_id) -> std::optional<KeyType> {
  BUSTUB_ASSERT(GetSize() == GetMaxSize(), "leaf full insert");
  auto pos_option = LowerBound(key, Com);
  int pos = GetSize();
  int size = GetSize();
  // 确定插入位置
  if (pos_option) {
    pos = pos_option.value();
    if (Com(key, key_array_[pos]) == 0) {
      return std::nullopt;
    }
  }
  // 进行临时拷贝
  std::vector<KeyType> tmp_key_array(size + 1);
  std::vector<ValueType> tmp_rid_array(size + 1);

  // 先拷贝  0 - pos, 值, pos - getsize
  std::copy(key_array_, key_array_ + pos, tmp_key_array.begin());
  tmp_key_array[pos] = key;
  std::copy(key_array_ + pos, key_array_ + size, tmp_key_array.begin() + pos + 1);

  std::copy(rid_array_, rid_array_ + pos, tmp_rid_array.begin());
  tmp_rid_array[pos] = value;
  std::copy(rid_array_ + pos, rid_array_ + size, tmp_rid_array.begin() + pos + 1);

  size_t left_size = (size + 1) / 2;
  size_t right_size = size + 1 - left_size;

  // 左边修改
  std::copy(tmp_key_array.begin(), tmp_key_array.begin() + left_size, key_array_);
  std::copy(tmp_rid_array.begin(), tmp_rid_array.begin() + left_size, rid_array_);
  SetSize(left_size);

  // 右边修改
  std::copy(tmp_key_array.begin() + left_size, tmp_key_array.end(), other_page->key_array_);
  std::copy(tmp_rid_array.begin() + left_size, tmp_rid_array.end(), other_page->rid_array_);
  other_page->SetSize(right_size);
  other_page->next_page_id_ = next_page_id_;
  next_page_id_ = other_page_id;  // 连接起来

  return other_page->key_array_[0];
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::LowerBound(const KeyType &key, const KeyComparator &Com) const -> std::optional<int> {
  // 没有元素 返回无
  if (GetSize() == 0) {
    return std::nullopt;
  }

  int l = 0, r = GetSize() - 1;  // 注意：这个地方要用int 因为-1可能出现负数
  std::optional<int> ans;

  while (l <= r) {
    int mid = (l + r) / 2;
    auto tmp = Com(key_array_[mid], key);
    if (tmp == 0) {
      return mid;
    } else if (tmp > 0) {
      ans = mid;
      r = mid - 1;
    } else {
      l = mid + 1;
    }
  }
  return ans;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key, const KeyComparator &Com) {
  auto option = LowerBound(key, Com);  // 返回>= key的第一个元素
  if (!option) {
    return;
  }
  auto pos = option.value();
  if (Com(key_array_[pos], key) != 0) {  // 没找到= 的
    return;
  }
  std::copy(key_array_ + pos + 1, key_array_ + GetSize(), key_array_ + pos);
  std::copy(rid_array_ + pos + 1, rid_array_ + GetSize(), rid_array_ + pos);
  ChangeSizeBy(-1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::IsFull() -> bool { return GetSize() == GetMaxSize(); }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Distribute(BPlusTreeLeafPage *other_leaf_page, bool i_am_left) -> const KeyType {
  // 重新分配的

  BUSTUB_ASSERT(GetSize() >= GetMinSize() + 1, "d1");
  if (i_am_left) {
    auto result = key_array_[GetSize() - 1];
    auto pre_id = rid_array_[GetSize() - 1];
    ChangeSizeBy(-1);
    // 调整other
    auto other_size = other_leaf_page->GetSize();
    std::copy_backward(other_leaf_page->key_array_, other_leaf_page->key_array_ + other_size,
                       other_leaf_page->key_array_ + other_size + 1);
    std::copy_backward(other_leaf_page->rid_array_, other_leaf_page->rid_array_ + other_size,
                       other_leaf_page->rid_array_ + other_size + 1);
    other_leaf_page->key_array_[0] = result;
    other_leaf_page->rid_array_[0] = pre_id;
    other_leaf_page->ChangeSizeBy(1);
    return other_leaf_page->key_array_[0];
  }

  // 我是右边给第一个
  auto result = key_array_[0];
  auto pre_id = rid_array_[0];
  std::copy(key_array_ + 1, key_array_ + GetSize(), key_array_);
  std::copy(rid_array_ + 1, rid_array_ + GetSize(), rid_array_);
  ChangeSizeBy(-1);

  auto other_size = other_leaf_page->GetSize();
  other_leaf_page->key_array_[other_size] = result;
  other_leaf_page->rid_array_[other_size] = pre_id;
  other_leaf_page->ChangeSizeBy(1);

  return key_array_[0];
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Merge(BPlusTreeLeafPage *other_leaf_page) {
  BUSTUB_ASSERT(GetSize() + other_leaf_page->GetSize() <= GetMaxSize(), "baiyu4");
  int other_size = other_leaf_page->GetSize();
  std::copy(other_leaf_page->key_array_, other_leaf_page->key_array_ + other_size, key_array_ + GetSize());
  std::copy(other_leaf_page->rid_array_, other_leaf_page->rid_array_ + other_size, rid_array_ + GetSize());

  next_page_id_ = other_leaf_page->next_page_id_;
  other_leaf_page->next_page_id_ = INVALID_PAGE_ID;

  other_leaf_page->SetSize(0);
  ChangeSizeBy(other_size);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
