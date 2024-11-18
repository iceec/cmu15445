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
  SetSize(0);
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
  size_t pos = UpperBound(key, Com);  // pos 一定>=1 找到> key的
  return page_id_array_[pos - 1];
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::UpperBound(const KeyType &key, const KeyComparator &Com) const -> size_t {
  size_t l = 1, r = GetSize();
  size_t ans = r + 1;

  while (l <= r) {
    size_t mid = (l + r) / 2;

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

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
