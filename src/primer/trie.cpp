#include "primer/trie.h"
#include <iostream>
#include <optional>
#include <stack>
#include <string_view>
#include "common/exception.h"
namespace bustub {

void f(std::string_view key) {
  if (key.size() == 0) return;  // root_
  std::stack<std::shared_ptr<const TrieNode>> node_stack;

  size_t n = key.size();

  std::shared_ptr<const TrieNode> node;
  std::shared_ptr<const TrieNode> next_node;
  for (size_t i = 0; i < n; ++i) {
    char elem = key[i];
    // 找到下一个结点
    auto it = node->children_.find(elem);

    if (it == node->children_.end()) return;  // 直接就是没找到

    node_stack.push(node);

    node = it->second;  // 下一个结点
  }
  // 出来表示 要删除的结点 保证是一定存在的 不然早就返回了
  // 删除的结点没有值 那么返回
  if (node->is_value_node_ == false) return;

  // 仅仅拷贝孩子就行了
  std::shared_ptr<TrieNode> new_node = std::make_shared<TrieNode>(node->children_);

  // 里面有值

  size_t pos = n - 1;
  while (node_stack.size()) {
    auto before_node = node_stack.top();
    node_stack.pop();
    std::shared_ptr<TrieNode> mid_node = std::move(before_node->Clone());
    mid_node->children_[key[pos]] = new_node;
    --pos;
    new_node = mid_node;
  }

  return;
}

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");
  //  返回一个shared_ptr  看看有没有最后一个treenode

  if (root_ == nullptr) return nullptr;

  if (key.size() == 0) {
    if (root_->is_value_node_ == false) return nullptr;

    auto result = std::dynamic_pointer_cast<const TrieNodeWithValue<T>>(root_);
    return (result == nullptr) ? nullptr : result->value_.get();
  }

  auto p = _Get<T>(root_, key, 0);  // 从 treenode 开始访问
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  return p == nullptr ? nullptr : p.get();
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");
  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
  // == 0 存放在root 里面
  // 递归的stack
  std::stack<std::shared_ptr<const TrieNode>> node_stack;
  // node 表示当前结点 nextnode 表示下一轮要访问的结点
  std::shared_ptr<const TrieNode> next_node;
  std::shared_ptr<const TrieNode> node = root_;
  size_t n = key.size();

  for (size_t i = 0; i < n; ++i) {
    if (node == nullptr)  // 表明当前没结点
    {
      node_stack.push(nullptr);
      next_node = nullptr;
    } else {  // 当前有个结点
      node_stack.push(node);
      auto it = node->children_.find(key[i]);  // 下一个结点的位置
      next_node = it == node->children_.end() ? nullptr : it->second;
    }
    node = next_node;  // 走向下一个结点
  }

  // 里面放的都是 node 都是前面的结点 现在要new一个新的结点了
  std::shared_ptr<TrieNode> put_value_node;
  // 这个地方不能clone  因为 一定是with value 所以 原来是node 不行的

  if (node == nullptr)
    put_value_node = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
  else  // 要把之前的孩子也给拷贝进来
    put_value_node = std::make_shared<TrieNodeWithValue<T>>(node->children_, std::make_shared<T>(std::move(value)));

  std::shared_ptr<const TrieNode> pre_node = put_value_node;
  // 站内的元素拿出来 进行新的clone
  size_t pos = n - 1;

  while (node_stack.size()) {
    auto mid_node = node_stack.top();
    node_stack.pop();

    std::shared_ptr<TrieNode> tmp_mid_node;

    if (mid_node)
      tmp_mid_node = std::move(
          mid_node->Clone());  // unique -> shared 这个地方一定要是 clone 因为不知道上一个结点到底是value or not
    else
      tmp_mid_node = std::make_shared<TrieNode>();

    tmp_mid_node->children_[key[pos]] = pre_node;  // 添加进入当前的node
    --pos;
    pre_node = tmp_mid_node;  // 切换 表明我是下一个
  }

  return Trie(pre_node);
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // throw NotImplementedException("Trie::Remove is not implemented.");
  //  You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  //  you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.

  std::stack<std::shared_ptr<const TrieNode>> node_stack;

  size_t n = key.size();

  std::shared_ptr<const TrieNode> node = root_;

  if (node == nullptr) return *this;
  for (size_t i = 0; i < n; ++i) {
    char elem = key[i];
    // 找到下一个结点
    auto it = node->children_.find(elem);

    if (it == node->children_.end()) return *this;  // 直接就是没找到

    node_stack.push(node);

    node = it->second;  // 下一个结点
  }
  // 出来表示 要删除的结点 保证是一定存在的 不然早就返回了
  // 删除的结点没有值 那么返回
  if (node->is_value_node_ == false) return *this;  // 返回本身就行

  // 仅仅拷贝孩子就行了 一定是一个trenode 所以要是没孩子 那么直接删除就好了
  // 如果没孩子了 那么本身 就为nullptr就可以了
  std::shared_ptr<TrieNode> new_node{nullptr};
  if (node->children_.size()) new_node = std::make_shared<TrieNode>(node->children_);

  size_t pos = n - 1;
  while (node_stack.size()) {
    auto before_node = node_stack.top();
    node_stack.pop();
    std::shared_ptr<TrieNode> mid_node = std::move(before_node->Clone());
    // 看看 之前的孩子 是不是需要删除 假如为nullptr表明上一个结点已经消失了
    if (new_node)
      mid_node->children_[key[pos]] = new_node;
    else
      mid_node->children_.erase(key[pos]);

    // 考察midnode  1：midnode本事有值 不能删除 或者还有孩子
    if (mid_node->is_value_node_ || mid_node->children_.size() > 0)
      new_node = mid_node;
    else
      new_node = nullptr;
    --pos;
  }

  return Trie(new_node);
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
