//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetMaxSize(max_size);
  SetSize(1);
  SetPageType(IndexPageType::INTERNAL_PAGE);
}
/*
 * Helper method to get/set the key associated with input "index" (a.k.a
 * array offset)
 */
// index从1开始 一直到 n-1
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  BUSTUB_ASSERT(index >= 1 && index < GetMaxSize(), "Internal Page not in range");
  return key_array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  BUSTUB_ASSERT(index >= 1 && index <= GetSize(), "Set Internal Page not in range");
  key_array_[index] = key;
}

/*
 * Helper method to get the value associated with input "index" (a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  BUSTUB_ASSERT(index >= 0 && index <= GetSize(), "Set Internal Page Value not in range");
  return page_id_array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindNextPageId(const KeyType &key, const KeyComparator &Com) const -> ValueType {
  if (GetSize() <= 1) {
    std::cout << "here" << std::endl;
  }
  BUSTUB_ASSERT(GetSize() > 1, "find next page id");  // 必须要有key的
  size_t pos = UpperBound(key, Com);                  // pos 一定>=1 找到> key的
  return page_id_array_[pos - 1];
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindNextPageId(Edge type) const -> ValueType {
  BUSTUB_ASSERT(GetSize() > 1, "most left");
  if (type == Edge::MostLeft) {
    return page_id_array_[0];
  }
  return page_id_array_[GetSize() - 1];
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::UpperBound(const KeyType &key, const KeyComparator &Com) const -> int {
  BUSTUB_ASSERT(GetSize() >= 2, "baiyu 19");
  int l = 1, r = GetSize() - 1;
  int ans = r + 1;

  while (l <= r) {
    int mid = (l + r) / 2;

    auto tmp = Com(key_array_[mid], key);

    if (tmp == 0) {
      return mid + 1;
    } else if (tmp > 0) {
      ans = mid;
      r = mid - 1;
    } else {
      l = mid + 1;
    }
  }
  BUSTUB_ASSERT(ans >= 1, "UpperBound ans < 1");
  return ans;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const Upinfo &value, const KeyComparator &Com) -> bool {
  BUSTUB_ASSERT(GetSize() < GetMaxSize(), "internal insert p1");

  int pos = UpperBound(value.first, Com);  // 寻找 > key 的第一个位置  ans >= 1

  if (pos > 1) {
    BUSTUB_ASSERT(Com(value.first, key_array_[pos - 1]) != 0, "internal insert p2");
  }

  // key 要在pos 这里  page_id要在pos这里
  // copy
  std::copy_backward(key_array_ + pos, key_array_ + GetSize() + 1, key_array_ + GetSize() + 2);
  std::copy_backward(page_id_array_ + pos, page_id_array_ + GetSize() + 1, page_id_array_ + GetSize() + 2);
  // 复制
  key_array_[pos] = value.first;
  page_id_array_[pos] = value.second;
  // 改变大小
  ChangeSizeBy(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FullInsert(const Upinfo &value, const KeyComparator &Com,
                                                BPlusTreeInternalPage *other_page) -> KeyType {
  BUSTUB_ASSERT(GetSize() == GetMaxSize(), "full internal insert p1");

  int pos = UpperBound(value.first, Com);

  if (pos > 1) {
    BUSTUB_ASSERT(Com(value.first, key_array_[pos - 1]) != 0, "full internal insert p2");
  }

  std::vector<KeyType> tmp_key_array(GetMaxSize() + 1);
  std::vector<ValueType> tmp_page_id_array(GetMaxSize() + 1);

  // 拷贝到临时的数组
  std::copy(key_array_ + 1, key_array_ + pos, tmp_key_array.begin() + 1);
  tmp_key_array[pos] = value.first;
  std::copy(key_array_ + pos, key_array_ + GetSize(), tmp_key_array.begin() + pos + 1);

  std::copy(page_id_array_, page_id_array_ + pos, tmp_page_id_array.begin());
  tmp_page_id_array[pos] = value.second;
  std::copy(page_id_array_ + pos, page_id_array_ + GetSize(), tmp_page_id_array.begin() + pos + 1);

  // 向上推送 中间的key pos 点
  // key  1 - result_pos      result_pos + 1 - end
  // pageid 0 - result_pos    result_pos - end
  size_t result_pos = 1 + GetMaxSize() / 2;
  auto result = tmp_key_array[result_pos];

  // 左边
  std::copy(tmp_key_array.begin() + 1, tmp_key_array.begin() + result_pos, key_array_ + 1);
  std::copy(tmp_page_id_array.begin(), tmp_page_id_array.begin() + result_pos, page_id_array_);
  SetSize(result_pos);

  // 右边
  std::copy(tmp_key_array.begin() + result_pos + 1, tmp_key_array.end(), other_page->key_array_ + 1);
  std::copy(tmp_page_id_array.begin() + result_pos, tmp_page_id_array.end(), other_page->page_id_array_);
  other_page->SetSize(tmp_key_array.size() - result_pos);

  return result;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::IsFull() const -> bool { return GetSize() == GetMaxSize(); }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FirstItem(const ValueType &left_value, const Upinfo &value) -> void {
  BUSTUB_ASSERT(GetSize() == 1, "First Item");

  page_id_array_[0] = left_value;
  page_id_array_[1] = value.second;
  key_array_[1] = value.first;

  ChangeSizeBy(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(const KeyType &key, const ValueType &value, const KeyComparator &Com) {
  // 到这个里面 一定是存在的
  int pos = KeyAt(key, Com);
  BUSTUB_ASSERT(value == page_id_array_[pos], "baiyu 6");
  // 消去
  std::copy(key_array_ + pos + 1, key_array_ + GetSize(), key_array_ + pos);
  std::copy(page_id_array_ + pos + 1, page_id_array_ + GetSize(), page_id_array_ + pos);
  ChangeSizeBy(-1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(const KeyType &key, const KeyComparator &Com) -> int {
  int pos = UpperBound(key, Com) - 1;
  BUSTUB_ASSERT(Com(key, key_array_[pos]) == 0, "p3");
  return pos;
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Replace(const KeyType &key, const KeyType &replace_key, const KeyComparator &Com) {
  int pos = KeyAt(key, Com);
  key_array_[pos] = replace_key;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Distribute(BPlusTreeInternalPage *other_internal_page, bool i_am_left,
                                                const KeyType &parent_key) -> const KeyType {
  BUSTUB_ASSERT(GetSize() >= GetMinSize() + 1, "d1");

  if (i_am_left) {
    auto result = key_array_[GetSize() - 1];
    auto pre_id = page_id_array_[GetSize() - 1];
    ChangeSizeBy(-1);
    auto other_size = other_internal_page->GetSize();
    std::copy_backward(other_internal_page->key_array_, other_internal_page->key_array_ + other_size,
                       other_internal_page->key_array_ + other_size + 1);
    std::copy_backward(other_internal_page->page_id_array_, other_internal_page->page_id_array_ + other_size,
                       other_internal_page->page_id_array_ + other_size + 1);

    other_internal_page->key_array_[1] = parent_key;
    other_internal_page->page_id_array_[0] = pre_id;
    other_internal_page->ChangeSizeBy(1);
    return result;
  }

  // 我是右边
  auto result = key_array_[1];
  auto pre_id = page_id_array_[0];
  std::copy(key_array_ + 1, key_array_ + GetSize(), key_array_);
  std::copy(page_id_array_ + 1, page_id_array_ + GetSize(), page_id_array_);

  other_internal_page->key_array_[other_internal_page->GetSize()] = parent_key;
  other_internal_page->page_id_array_[other_internal_page->GetSize()] = pre_id;
  ChangeSizeBy(-1);
  other_internal_page->ChangeSizeBy(1);
  return result;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Merge(BPlusTreeInternalPage *other_internal_page, const KeyType &parent_key) {
  BUSTUB_ASSERT(GetSize() + other_internal_page->GetSize() <= GetMaxSize(), "baiyu 8");
  int other_size = other_internal_page->GetSize();

  key_array_[GetSize()] = parent_key;
  std::copy(other_internal_page->key_array_ + 1, other_internal_page->key_array_ + other_size,
            key_array_ + GetSize() + 1);
  std::copy(other_internal_page->page_id_array_, other_internal_page->page_id_array_ + other_size,
            page_id_array_ + GetSize());

  ChangeSizeBy(other_size);
  other_internal_page->SetSize(1);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
