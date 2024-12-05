/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  if (leaf_page_id_ == INVALID_PAGE_ID) {
    return true;
  }

  auto read_page_guard = bpm_->CheckedReadPage(leaf_page_id_).value();
  auto read_page = read_page_guard.As<LeafPage>();

  return read_page->GetNextPageId() == INVALID_PAGE_ID && pos_ == read_page->GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> std::pair<const KeyType &, const ValueType &> {
  BUSTUB_ASSERT(leaf_page_id_ != INVALID_PAGE_ID, "is end");
  auto read_page_guard = bpm_->CheckedReadPage(leaf_page_id_).value();
  auto read_page = read_page_guard.As<LeafPage>();
  BUSTUB_ASSERT(pos_ < read_page->GetSize(), "baiyu 15");
  return {read_page->KeyAt(pos_), read_page->ValueAt(pos_)};
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, page_id_t leaf_page_id, int pos)
    : bpm_(bpm), leaf_page_id_(leaf_page_id), pos_(pos) {}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  auto read_page_guard = bpm_->CheckedReadPage(leaf_page_id_).value();
  auto read_page = read_page_guard.As<LeafPage>();
  if (++pos_ < read_page->GetSize()) {
    return *this;
  }
  // pos_ == getsize();
  if (read_page->GetNextPageId() == INVALID_PAGE_ID) {
    return *this;
  }
  leaf_page_id_ = read_page->GetNextPageId();
  pos_ = 0;
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
