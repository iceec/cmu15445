//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_internal_page.h
//
// Copyright (c) 2018-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>
#include <string>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_INTERNAL_PAGE_TYPE BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>
#define INTERNAL_PAGE_HEADER_SIZE 12
#define INTERNAL_PAGE_SLOT_CNT \
  ((BUSTUB_PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / ((int)(sizeof(KeyType) + sizeof(ValueType))))  // NOLINT

enum class Edge {
  MostLeft = 0,
  MostRight,
};

/**
 * Store `n` indexed keys and `n + 1` child pointers (page_id) within internal page.
 * Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
 * K(i) <= K < K(i+1).
 * NOTE: Since the number of keys does not equal to number of child pointers,
 * the first key in key_array_ always remains invalid. That is to say, any search / lookup
 * should ignore the first key.
 *
 * Internal page format (keys are stored in increasing order):
 *  ---------
 * | HEADER |
 *  ---------
 *  ------------------------------------------
 * | KEY(1)(INVALID) | KEY(2) | ... | KEY(n) |
 *  ------------------------------------------
 *  ---------------------------------------------
 * | PAGE_ID(1) | PAGE_ID(2) | ... | PAGE_ID(n) |
 *  ---------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeInternalPage : public BPlusTreePage {
 public:
  // Delete all constructor / destructor to ensure memory safety
  BPlusTreeInternalPage() = delete;
  BPlusTreeInternalPage(const BPlusTreeInternalPage &other) = delete;

  using Upinfo = std::pair<KeyType, ValueType>;

  /**
   * Writes the necessary header information to a newly created page, must be called after
   * the creation of a new page to make a valid `BPlusTreeInternalPage`
   * @param max_size Maximal size of the page
   */
  void Init(int max_size = INTERNAL_PAGE_SLOT_CNT);

  /**
   * @param index The index of the key to get. Index must be non-zero.
   * @return Key at index
   */
  auto KeyAt(int index) const -> KeyType;

  auto KeyAt(const KeyType &key, const KeyComparator &Com) -> int;

  /**
   * @param index The index of the key to set. Index must be non-zero.
   * @param key The new value for key
   */
  void SetKeyAt(int index, const KeyType &key);

  /**
   * @param value The value to search for
   * @return The index that corresponds to the specified value
   */
  auto ValueIndex(const ValueType &value) const -> int;

  /**
   * @param index The index to search for
   * @return The value at the index
   */
  auto ValueAt(int index) const -> ValueType;

  /**
   * @brief For test only, return a string representing all keys in
   * this internal page, formatted as "(key1,key2,key3,...)"
   *
   * @return The string representation of all keys in the current internal page
   */
  auto ToString() const -> std::string {
    std::string kstr = "(";
    bool first = true;

    // First key of internal page is always invalid
    for (int i = 1; i < GetSize(); i++) {
      KeyType key = KeyAt(i);
      if (first) {
        first = false;
      } else {
        kstr.append(",");
      }

      kstr.append(std::to_string(key.ToString()));
    }
    kstr.append(")");

    return kstr;
  }
  auto FindNextPageId(const KeyType &key, const KeyComparator &Com) const -> ValueType;

  auto Insert(const Upinfo &value, const KeyComparator &Com) -> bool;

  auto FullInsert(const Upinfo &value, const KeyComparator &Com, BPlusTreeInternalPage *other_page) -> KeyType;

  auto IsFull() const -> bool;

  auto FirstItem(const ValueType &left_value, const Upinfo &value) -> void;

  auto FindNextPageId(Edge type) const -> ValueType;

  void Remove(const KeyType &key, const ValueType &value, const KeyComparator &Com);

  // 返回最后一个> key的pos(pos>=1)
  auto UpperBound(const KeyType &key, const KeyComparator &Com) const -> int;

  void Replace(const KeyType &key, const KeyType &replace_key, const KeyComparator &Com);

  auto Distribute(BPlusTreeInternalPage *other_internal_page, bool i_am_left,
                  const KeyType &parent_key) -> const KeyType;

  // zuo mer you
  void Merge(BPlusTreeInternalPage *other_internal_page, const KeyType &parent_key);

  auto Empty() -> bool { return GetSize() == 1; }

 private:
  // Array members for page data.
  // key 从key[1]开始存 一直存到 key[n-1] 一共 n-1个key
  // page_id 从page[0]开始存 一直存到page_id[n-1] 一共n个id
  // 每个key 左右都要有左子树和右子树为 id-1 and id
  // size always < max_size
  KeyType key_array_[INTERNAL_PAGE_SLOT_CNT];
  ValueType page_id_array_[INTERNAL_PAGE_SLOT_CNT];
  // (Fall 2024) Feel free to add more fields and helper functions below if needed
};

}  // namespace bustub
