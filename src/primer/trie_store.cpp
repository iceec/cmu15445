#include "primer/trie_store.h"
#include "common/exception.h"

namespace bustub {

template <class T>
auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<T>> {
  // Pseudo-code:
  // (1) Take the root lock, get the root, and release the root lock. Don't lookup the value in the
  //     trie while holding the root lock.
  // (2) Lookup the value in the trie.
  // (3) If the value is found, return a ValueGuard object that holds a reference to the value and the
  //     root. Otherwise, return std::nullopt.
  // throw NotImplementedException("TrieStore::Get is not implemented.");

  Trie tmp;
  // 先对于 根节点做出一个copy
  {
    std::lock_guard<std::mutex> lock(root_lock_);
    tmp = root_;
  }

  auto pointer = tmp.Get<T>(key);  // 获得原始指针

  if (pointer == nullptr) return std::nullopt;  // 表明没有相应的key

  return ValueGuard<T>(tmp, *pointer);  // 这个时候释放不了 这个pointer
}

template <class T>
void TrieStore::Put(std::string_view key, T value) {
  // You will need to ensure there is only one writer at a time. Think of how you can achieve this.
  // The logic should be somehow similar to `TrieStore::Get`.
  // throw NotImplementedException("TrieStore::Put is not implemented.");
  // only on writer in trie 整个过程lock_guard 写锁
  // 先获取原本的trie  读锁
  // 在上面修改
  // 然后修改本身的root = 现在的root就行了 读锁

  std::lock_guard<std::mutex> w_lock(write_lock_);

  Trie tmp;
  {
    std::lock_guard<std::mutex> r_lock(root_lock_);
    tmp = root_;
  }

  tmp = tmp.Put(key, std::move(value));  // value记得进行移动才行 因为可能不能拷贝

  {
    std::lock_guard<std::mutex> r_lock(root_lock_);
    root_ = tmp;
  }
}

void TrieStore::Remove(std::string_view key) {
  // You will need to ensure there is only one writer at a time. Think of how you can achieve this.
  // The logic should be somehow similar to `TrieStore::Get`.
  // throw NotImplementedException("TrieStore::Remove is not implemented.");

  std::lock_guard<std::mutex> w_lock(write_lock_);

  Trie tmp;
  {
    std::lock_guard<std::mutex> r_lock(root_lock_);
    tmp = root_;
  }

  tmp = tmp.Remove(key);  // 直接移除就好了

  {
    std::lock_guard<std::mutex> r_lock(root_lock_);
    root_ = tmp;
  }
}

// Below are explicit instantiation of template functions.

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<uint32_t>>;
template void TrieStore::Put(std::string_view key, uint32_t value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<std::string>>;
template void TrieStore::Put(std::string_view key, std::string value);

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<Integer>>;
template void TrieStore::Put(std::string_view key, Integer value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<MoveBlocked>>;
template void TrieStore::Put(std::string_view key, MoveBlocked value);

}  // namespace bustub
