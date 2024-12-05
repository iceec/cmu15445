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
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  Context ctx;
  ctx.read_set_.push_back(bpm_->CheckedReadPage(header_page_id_).value());
  auto read_page = ctx.read_set_.back().As<BPlusTreeHeaderPage>();
  return read_page->root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
// need to do 不同版本
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::FindKeyWithWriteGuard(const KeyType &key, Context &ctx, page_id_t root_page_id) {
  BUSTUB_ASSERT(root_page_id != INVALID_PAGE_ID, "write find key");

  auto &que = ctx.write_set_;
  que.push_back(bpm_->CheckedWritePage(root_page_id).value());
  auto basic_page = que.back().As<BPlusTreePage>();
  /**
   * 考虑插入的情况 自己不会影响上层 那么根节点什么时候插入不影响上层就是size < max_size
   * 先将所有的东西都装进来然后从底到顶检查
   */
  while (!basic_page->IsLeafPage()) {
    auto internal_page = que.back().As<InternalPage>();
    page_id_t next_page_id = internal_page->FindNextPageId(key, comparator_);
    que.push_back(bpm_->CheckedWritePage(next_page_id).value());
    basic_page = que.back().As<BPlusTreePage>();
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SafePopForInsert(Context &ctx) {
  auto &que = ctx.write_set_;
  // pos = 0 是 header page 所以 不能管
  auto pos = que.size() - 1;
  BUSTUB_ASSERT(pos > 0, "baiyu 10");
  while (pos > 0) {
    auto basic_page = que[pos].As<BPlusTreePage>();
    if (basic_page->SafeInsert()) {
      break;
    }
    --pos;
  }
  // pos 所处的位置是安全的 或者都不安全 pos = 0  不会影响上面 那么 [0 pos - 1]都释放了把
  for (size_t i = 0; i + 1 <= pos; ++i) {
    que.pop_front();
  }
  BUSTUB_ASSERT(que.size() >= 1, "baiyu 11");
  // 表明 连根节点都不是安全的
  if (pos == 0) {
    ctx.header_page_ = std::move(que.front());
    que.pop_front();
  }
  return;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::FindKeyWithReadGuard(const KeyType &key, Context &ctx, page_id_t root_page_id) {
  BUSTUB_ASSERT(root_page_id != INVALID_PAGE_ID, "write find key");

  auto &que = ctx.read_set_;
  que.push_back(bpm_->CheckedReadPage(root_page_id).value());
  auto basic_page = que.back().As<BPlusTreePage>();

  while (!basic_page->IsLeafPage()) {
    auto internal_page = que.back().As<InternalPage>();
    page_id_t next_page_id = internal_page->FindNextPageId(key, comparator_);
    que.push_back(bpm_->CheckedReadPage(next_page_id).value());
    basic_page = que.back().As<BPlusTreePage>();
  }
}

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
  FindKeyWithReadGuard(key, ctx, header_page->root_page_id_);
  // ctx.read_set.back就是叶子了
  auto leaf_type_page = ctx.read_set_.back().As<LeafPage>();
  auto ans = leaf_type_page->FindMatchValue(key, comparator_);  // Bai to do
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
  auto &write_que = ctx.write_set_;
  write_que.push_back(bpm_->CheckedWritePage(header_page_id_).value());
  auto header_page = write_que.back().AsMut<BPlusTreeHeaderPage>();
  // 本身树空 root_page_Id == invaild
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    auto root_page_id = bpm_->NewPage();
    write_que.push_back(bpm_->CheckedWritePage(root_page_id).value());
    auto root_page = write_que.back().AsMut<LeafPage>();
    root_page->Init(leaf_max_size_);
    auto result = root_page->Insert(key, value, comparator_);  // 单独插入
    BUSTUB_ASSERT(result, "insert p1");
    header_page->root_page_id_ = root_page_id;
    return true;
  }

  // 找寻叶子节点 刨除了安全节点 假如根节点是不安全的那么会
  FindKeyWithWriteGuard(key, ctx, header_page->root_page_id_);
  SafePopForInsert(ctx);

  // 向叶子节点插入key value
  auto leaf_type_page = write_que.back().AsMut<LeafPage>();
  // 叶子节点没满
  if (!leaf_type_page->IsFull()) {
    return leaf_type_page->Insert(key, value, comparator_);
  }

  // 生成新的页面并初始化
  std::pair<KeyType, page_id_t> up_value{};  // 向上层节点提供的值
  {
    auto new_leaf_page_id = bpm_->NewPage();
    auto new_leaf_page_guard = bpm_->CheckedWritePage(new_leaf_page_id).value();
    auto new_leaf_page = new_leaf_page_guard.AsMut<LeafPage>();
    new_leaf_page->Init(leaf_max_size_);
    // result 返回一个optional  key 假如有相同值得话 返回std::nullopt
    auto result = leaf_type_page->FullInsert(key, value, comparator_, new_leaf_page, new_leaf_page_id);

    if (!result) {
      return false;
    }
    up_value.first = result.value();
    up_value.second = new_leaf_page_id;
  }

  // 需要向上进行分裂
  write_que.pop_back();
  while (!write_que.empty()) {
    auto internal_type_page = write_que.back().AsMut<InternalPage>();
    /**
     * 考虑是获取page_id还是page_guard 现在的想法是获取page_id
     * 获取page_id 因为我现在在改变树 能访问到这个page的整个路径是都被我锁住的 所以没人可以访问的到
     * 修改得话考虑新生成得放入ctx当中
     */
    if (!internal_type_page->IsFull()) {
      return internal_type_page->Insert(up_value, comparator_);  // 一定返回true busassert 返回false
    }

    {
      auto new_internal_page_id = bpm_->NewPage();
      auto new_internal_page_guard = bpm_->CheckedWritePage(new_internal_page_id).value();
      auto new_internal_page = new_internal_page_guard.AsMut<InternalPage>();
      new_internal_page->Init(internal_max_size_);
      auto result = internal_type_page->FullInsert(up_value, comparator_, new_internal_page);
      up_value.first = result;
      up_value.second = new_internal_page_id;
    }

    write_que.pop_back();
  }
  // 表明需要改header
  if (ctx.header_page_) {
    // 就连根节点都已经分裂了 那么新搞一个中间节点把root_page_id放左 result_page_id 放右 对应key 为 result key
    header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
    auto new_root_page_id = bpm_->NewPage();
    auto new_root_write_page_guard = bpm_->CheckedWritePage(new_root_page_id).value();
    auto internal_type_page = new_root_write_page_guard.AsMut<InternalPage>();
    internal_type_page->Init(internal_max_size_);
    internal_type_page->FirstItem(header_page->root_page_id_, up_value);
    header_page->root_page_id_ = new_root_page_id;
  }

  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SafePopForRemove(Context &ctx) {
  auto &que = ctx.write_set_;
  // pos = 0 是 header page 所以 不能管
  auto pos = que.size() - 1;
  BUSTUB_ASSERT(pos > 0, "baiyu 11");
  // pos = 0 是 header
  // pos = 1 是根节点 根节点的safe为非empty 其他界节点的safe是size > getminsize
  while (pos > 1) {
    auto basic_page = que[pos].As<BPlusTreePage>();
    if (basic_page->SafeRemove()) {
      break;
    }
    --pos;
  }
  // pos = 1 表明根节点的下面都不安全是判断size > 1
  if (pos == 1) {
    auto basic_page = que[pos].As<BPlusTreePage>();
    if (basic_page->IsLeafPage() && basic_page->GetSize() <= 1) {
      BUSTUB_ASSERT(basic_page->GetSize() == 1, "baiyu 16");
      --pos;
    } else if (!basic_page->IsLeafPage() && basic_page->GetSize() <= 2) {
      BUSTUB_ASSERT(basic_page->GetSize() == 2, "baiyu 17");
      --pos;
    }
  }

  // pos 所处的位置是安全的 或者都不安全 pos = 0  不会影响上面 那么 [0 pos - 1]都释放了把
  for (size_t i = 0; i + 1 <= pos; ++i) {
    que.pop_front();
  }
  BUSTUB_ASSERT(que.size() >= 1, "baiyu 11");
  // 表明 连根节点都不是安全的
  if (pos == 0) {
    ctx.header_page_ = std::move(que.front());
    que.pop_front();
  }
  return;
}

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
  // 判断根节点是不是空的
  ctx.write_set_.push_back(bpm_->CheckedWritePage(header_page_id_).value());
  auto header_page = ctx.write_set_.back().AsMut<BPlusTreeHeaderPage>();
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    return;
  }
  // find 最后的根节点 全程write back是叶子节点
  FindKeyWithWriteGuard(key, ctx, header_page->root_page_id_);
  SafePopForRemove(ctx);
  // 先将元素进行删除
  auto &write_que = ctx.write_set_;
  auto leaf_page = write_que.back().AsMut<LeafPage>();
  auto leaf_page_id = write_que.back().GetPageId();
  if (!leaf_page->Remove(key, comparator_)) {
    return;  // 注意这个地方很可能remove失败的 因为是并行的
  }

  /**
   * 叶子节点是根节点 那么删除后 两种情况
   *    1：还有元素就保留
   *    2：没有元素要删除这一页然后header_page删除就行了
   */
  // remove成功了 并且是根节点
  if (write_que.size() == 1) {
    if (ctx.header_page_) {
      BUSTUB_ASSERT(leaf_page->Empty(), "baiyu 18");
      auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
      write_que.pop_back();  // 必须先释放啊 不然不能删除
      BUSTUB_ASSERT(bpm_->DeletePage(header_page->root_page_id_), "baiyu 9");
      header_page->root_page_id_ = INVALID_PAGE_ID;
    } else {
      BUSTUB_ASSERT(!leaf_page->Empty(), "baiyu 19");
    }
    return;
  }
  /**
   * 叶子节点非根节点 删除元素后判断是否需要重组 不需要就返回 删除的话
   *  肯定有左边或者右边的或者都有 把page_id那上去 判断可不可以合并或者分配 返回page_id and K
   */
  // 这句话应该永远不会执行 考虑删去
  if (!leaf_page->Few()) {
    return;
  }
  BUSTUB_ASSERT(write_que.size() >= 2, "baiyu12");
  auto parent_internal_page = write_que.at(write_que.size() - 2).AsMut<InternalPage>();

  auto info = MergeOrRedistribution(parent_internal_page, key, leaf_page_id, leaf_page->GetSize());

  auto other_leaf_page_guard = std::move(info.page_guard_);
  auto other_leaf_page = other_leaf_page_guard.template AsMut<LeafPage>();
  auto other_leaf_page_id = other_leaf_page_guard.GetPageId();

  // distribute 重新分配
  if (!info.merge_) {
    auto replace_key = other_leaf_page->Distribute(leaf_page, info.left_);
    parent_internal_page->Replace(info.parent_key_, replace_key, comparator_);
    return;
  }
  // merge 固定删除右边的 左边的去合并右边的然后删除右边的一页
  page_id_t delete_page_id = leaf_page_id;
  page_id_t child_page_id = leaf_page_id;
  if (info.left_) {
    other_leaf_page->Merge(leaf_page);
    child_page_id = other_leaf_page_id;
    write_que.back().Drop();  // 可以drop因为我拿着parent呢
  } else {
    leaf_page->Merge(other_leaf_page);
    delete_page_id = other_leaf_page_id;
    other_leaf_page_guard.Drop();
  }
  BUSTUB_ASSERT(bpm_->DeletePage(delete_page_id), "delet page_id");
  auto &delete_key = info.parent_key_;
  // 这个时候 根节点已经匹配好了 可以释放 给index去遍历了 other page 也可以释放了
  write_que.pop_back();
  return RemoveFromInternal(ctx, delete_key, delete_page_id, child_page_id);  // Bai to do
}
// internal 要进行 delete了
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromInternal(Context &ctx, const KeyType &key, const page_id_t page_id,
                                        const page_id_t down_page_id) {
  auto internal_page = ctx.write_set_.back().AsMut<InternalPage>();
  auto internal_page_id = ctx.write_set_.back().GetPageId();
  // 假如都删除第一个叶子节点 那么不可能到这还有两个把
  internal_page->Remove(key, page_id, comparator_);  // 考虑不需要page id 的安全考虑

  // 最后一个节点表明这个节点要是安全的
  // 除非有header 到这个里面删除一定成功那么要是
  if (ctx.write_set_.size() == 1) {
    if (ctx.header_page_) {
      BUSTUB_ASSERT(internal_page->Empty(), "baiyu 21");
      auto header_page = ctx.header_page_.value().AsMut<BPlusTreeHeaderPage>();
      BUSTUB_ASSERT(ctx.write_set_.back().GetPageId() == header_page->root_page_id_, "baiyu 7");
      ctx.write_set_.pop_back();
      bpm_->DeletePage(header_page->root_page_id_);
      header_page->root_page_id_ = down_page_id;
    }
    BUSTUB_ASSERT(!internal_page->Empty(), "baiyu 22");
    return;
  }
  /**
   * 上面还有节点呢
   */
  // 这个应该也不会执行的
  if (!internal_page->Few()) {
    return;
  }
  auto &write_que = ctx.write_set_;
  BUSTUB_ASSERT(write_que.size() >= 2, "baiyu12");
  auto parent_internal_page = write_que.at(write_que.size() - 2).AsMut<InternalPage>();

  MergeOrDistributionInfo info =
      MergeOrRedistribution(parent_internal_page, key, internal_page_id, internal_page->GetSize());

  auto other_internal_page_guard = std::move(info.page_guard_);
  auto other_internal_page = other_internal_page_guard.template AsMut<InternalPage>();
  auto other_internal_page_id = other_internal_page_guard.GetPageId();

  // distribute 重新分配
  if (!info.merge_) {
    auto replace_key = other_internal_page->Distribute(internal_page, info.left_, info.parent_key_);
    parent_internal_page->Replace(info.parent_key_, replace_key, comparator_);
    return;
  }
  // merge 固定删除右边的 左边的去合并右边的然后删除右边的一页
  page_id_t delete_page_id = internal_page_id;
  page_id_t child_page_id = internal_page_id;
  if (info.left_) {
    other_internal_page->Merge(internal_page, info.parent_key_);
    child_page_id = other_internal_page_id;
    write_que.back().Drop();
  } else {
    internal_page->Merge(other_internal_page, info.parent_key_);
    delete_page_id = other_internal_page_id;
    other_internal_page_guard.Drop();
  }
  BUSTUB_ASSERT(bpm_->DeletePage(delete_page_id), "delet page_id");
  auto &delete_key = info.parent_key_;
  // 这个时候 根节点已经匹配好了 可以释放 给index去遍历了
  write_que.pop_back();
  return RemoveFromInternal(ctx, delete_key, delete_page_id, child_page_id);
}
// 注意这里的key 可能不是完全匹配的 不能用KeyAt
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::MergeOrRedistribution(InternalPage *parent_internal_page, const KeyType &key,
                                           const page_id_t page_id, int page_size) -> MergeOrDistributionInfo {
  int pos = parent_internal_page->UpperBound(key, comparator_) - 1;  // 返回的pos对应key的page_Id的位置

  std::optional<WritePageGuard> left_page_guard;
  std::optional<WritePageGuard> right_page_guard;

  if (pos - 1 >= 0) {
    left_page_guard = bpm_->CheckedWritePage(parent_internal_page->ValueAt(pos - 1));
  }

  if (pos + 1 < parent_internal_page->GetSize()) {
    right_page_guard = bpm_->CheckedWritePage(parent_internal_page->ValueAt(pos + 1));
  }

  if (left_page_guard) {
    auto left_page = left_page_guard->As<BPlusTreePage>();
    if (left_page->GetSize() + page_size <= left_page->GetMaxSize()) {
      return {std::move(left_page_guard.value()), true, true, parent_internal_page->KeyAt(pos)};
    }
    if (left_page->GetSize() >= left_page->GetMinSize() + 1) {
      return {std::move(left_page_guard.value()), true, false, parent_internal_page->KeyAt(pos)};
    }
  }
  BUSTUB_ASSERT(right_page_guard, "");

  auto right_page = right_page_guard->As<BPlusTreePage>();
  if (right_page->GetSize() + page_size <= right_page->GetMaxSize()) {
    return {std::move(right_page_guard.value()), false, true, parent_internal_page->KeyAt(pos + 1)};
  }
  BUSTUB_ASSERT(right_page->GetSize() >= right_page->GetMinSize() + 1, "merge or dis p2");
  return {std::move(right_page_guard.value()), false, false, parent_internal_page->KeyAt(pos + 1)};
}

/*****************************************************************************
 * INDEX ITERATOR Bai todo more pretty
 *****************************************************************************/

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Find(Edge type) -> INDEXITERATOR_TYPE {
  Context ctx;
  ctx.read_set_.push_back(bpm_->CheckedReadPage(header_page_id_).value());
  auto header_page = ctx.read_set_.back().As<BPlusTreeHeaderPage>();
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    return IndexIterator<KeyType, ValueType, KeyComparator>(bpm_, INVALID_PAGE_ID, -1);
  }
  page_id_t page_id = header_page->root_page_id_;
  ctx.read_set_.push_back(bpm_->CheckedReadPage(page_id).value());

  auto basic_page = ctx.read_set_.back().As<BPlusTreePage>();

  while (!basic_page->IsLeafPage()) {
    auto internal_page = ctx.read_set_.back().As<InternalPage>();
    page_id = internal_page->FindNextPageId(type);
    ctx.read_set_.push_back(bpm_->CheckedReadPage(page_id).value());
    basic_page = ctx.read_set_.back().As<BPlusTreePage>();
  }

  if (type == Edge::MostLeft) {
    return IndexIterator<KeyType, ValueType, KeyComparator>(bpm_, page_id, 0);
  }

  auto leaf_page = ctx.read_set_.back().As<LeafPage>();

  return IndexIterator<KeyType, ValueType, KeyComparator>(bpm_, page_id, leaf_page->GetSize());
}

/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return Find(Edge::MostLeft); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  Context ctx;
  ctx.read_set_.push_back(bpm_->CheckedReadPage(header_page_id_).value());
  auto header_page = ctx.read_set_.back().As<BPlusTreeHeaderPage>();
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    return IndexIterator<KeyType, ValueType, KeyComparator>(bpm_, INVALID_PAGE_ID, -1);
  }
  FindKeyWithReadGuard(key, ctx, header_page->root_page_id_);
  // 找到叶子节点
  auto leaf_page = ctx.read_set_.back().As<LeafPage>();
  auto page_id = ctx.read_set_.back().GetPageId();

  auto option = leaf_page->LowerBound(key, comparator_);
  int pos = leaf_page->GetSize();
  if (option) {
    pos = option.value();
  }

  return IndexIterator<KeyType, ValueType, KeyComparator>(bpm_, page_id, pos);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return Find(Edge::MostRight); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  Context ctx;
  ctx.read_set_.push_back(bpm_->CheckedReadPage(header_page_id_).value());

  auto header_page = ctx.read_set_.back().As<BPlusTreeHeaderPage>();

  return header_page->root_page_id_;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
