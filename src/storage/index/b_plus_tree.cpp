#include "storage/index/b_plus_tree.h"
#include "storage/index/b_plus_tree_debug.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return true; }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 * 过程当中只掌握着当前节点的读锁就OK了 ???考虑是不是会出现bug ??? 考虑deque存储所有路径读锁
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
  // Declaration of context instance.
  Context ctx;
  auto header_page_guard = bpm_->CheckedReadPage(header_page_id_);
  auto header_page = header_page_guard->As<BPlusTreeHeaderPage>();

  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    return false;
  }
  ReadPageGuard page_guard;

  page_id_t node_page_id = header_page->root_page_id_;

  page_guard = bpm_->CheckedReadPage(node_page_id).value();

  auto basic_type_node_page = page_guard.As<BPlusTreePage>();

  while (!basic_type_node_page->IsLeafPage()) {
    auto internal_type_node_page = page_guard.As<InternalPage>();
    if ((node_page_id = internal_type_node_page->FindNextPageId(key,comparator_)) == INVALID_PAGE_ID) {  // Bai todo
      return false;
    }
    page_guard = bpm_->CheckedReadPage(node_page_id).value();  // 对前者进行drop了
    basic_type_node_page = page_guard.As<BPlusTreePage>();
  }
  // 出来表示是叶子结点了
  auto leaf_type_page = page_guard.As<LeafPage>();
  auto ans = leaf_type_page->FindMatchValue(key,comparator_);  // Bai to do
  if (ans) {
    result->push_back(ans.value());
    return true;
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return 0; }

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
