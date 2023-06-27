#ifndef SURF_H_
#define SURF_H_

#include <string>
#include <type_traits>
#include <vector>

#include "config.hpp"
#include "fst_builder.hpp"
#include "louds_dense.hpp"
#include "louds_sparse.hpp"

namespace fst {

class FST {
 public:
  class Iter {
   public:
    Iter() = default;

    explicit Iter(const FST *filter) {
      dense_iter_ = LoudsDense::Iter(filter->louds_dense_.get());
      sparse_iter_ = LoudsSparse::Iter(filter->louds_sparse_.get());
    }

    void clear();

    bool isValid() const;

    int compare(const std::string &key) const;

    uint64_t getValue() const;

    std::string getKey() const;

    // Returns true if the status of the iterator after the operation is valid
    bool operator++(int);

    bool operator--(int);

    bool operator!=(const Iter &);

   private:
    void passToSparse();

    bool incrementDenseIter();

    bool incrementSparseIter();

    bool decrementDenseIter();

    bool decrementSparseIter();

   private:
    // true implies that dense_iter_ is valid
    LoudsDense::Iter dense_iter_;
    LoudsSparse::Iter sparse_iter_;

    friend class FST;
  };

 public:
  FST() = default;

  //------------------------------------------------------------------
  // Input keys must be SORTED
  //------------------------------------------------------------------
  FST(const std::vector<std::string> &keys, const std::vector<uint64_t> &values) {
    create(keys, values, kIncludeDense, kSparseDenseRatio);
  }

  FST(const std::vector<uint32_t> &offsets, const std::vector<uint64_t> &values, const uint8_t *data) {
    keys_.reserve(offsets.size());

    for (auto offset: offsets) {
      uint8_t key_length = data[offset];
      std::string key(reinterpret_cast<const char *>(data) + offset + 1, key_length);
      keys_.emplace_back(move(key));
    }

    create(keys_, values, kIncludeDense, kSparseDenseRatio);
  }

  FST(const std::vector<uint64_t> &keys, const std::vector<uint64_t> &values) {
    std::vector<std::string> transformed_keys;
    transformed_keys.reserve(keys.size());

    for (auto key : keys) {
      uint64_t endian_swapped_word = __builtin_bswap64(key);
      transformed_keys.emplace_back(std::string(reinterpret_cast<const char *>(&endian_swapped_word), 8));
    }
    create(transformed_keys, values, kIncludeDense, kSparseDenseRatio);
  }

  FST(const std::vector<uint32_t> &keys, const std::vector<uint64_t> &values) {
    std::vector<std::string> transformed_keys;
    transformed_keys.reserve(keys.size());

    for (auto key : keys) {
      uint32_t endian_swapped_word = __builtin_bswap32(key);
      transformed_keys.emplace_back(std::string(reinterpret_cast<const char *>(&endian_swapped_word), 4));
    }
    create(transformed_keys, values, kIncludeDense, kSparseDenseRatio);
  }

  FST(const std::vector<std::string> &keys, const std::vector<uint64_t> &values, const bool include_dense,
      const uint32_t sparse_dense_ratio) {
    create(keys, values, include_dense, sparse_dense_ratio);
  }

  ~FST() = default;

  void create(const std::vector<std::string> &keys, const std::vector<uint64_t> &values, bool include_dense,
              uint32_t sparse_dense_ratio);

  bool lookupKey(const std::string &key, uint64_t &value) const;

  bool lookupKey(uint32_t key, uint64_t &value) const;

  bool lookupKey(uint64_t key, uint64_t &value) const;

  // this function is used by hybrid trie to continue a search started in ARTHybrid
  inline bool lookupKeyAtNode(const char *key, uint64_t key_length, level_t level, size_t node_number,
                              uint64_t &value) const;

  // this function is used by hybrid trie to execute one lookup step in AMAC setting
  // returns true if next nodenumber is found or lookup finished
  // returns false if this key does not exist
  inline bool amacLookup(const char keyByte, level_t level, size_t &node_number) const;

  void getNode(level_t level, size_t node_number, std::vector<uint8_t> &lables, std::vector<uint64_t> &values,
               std::vector<uint8_t> &prefix, std::vector<uint64_t> &fst_node_numbers) const;

  uint64_t lookupNodeNum(const char *key, uint64_t key_length) const;

  std::pair<bool, uint64_t> lookupNodeNumOption(const char *key, uint64_t key_length) const;

  void moveToLeftmostKeyStartingAtNode(level_t level, size_t node_number, FST::Iter& iter) const;

  FST::Iter moveToKeyStartingAtNode(level_t &level, size_t node_number, const std::string &key) const;

  // This function searches in a conservative way: if inclusive is true
  // and the stored key prefix matches key, iter stays at this key prefix.
  FST::Iter moveToKeyGreaterThan(const std::string &key, bool inclusive) const;

  FST::Iter moveToKeyLessThan(const std::string &key, bool inclusive) const;

  FST::Iter moveToFirst() const;

  FST::Iter moveToLast() const;

  std::pair<FST::Iter, FST::Iter> lookupRange(const std::string &left_key, bool left_inclusive,
                                              const std::string &right_key, bool right_inclusive);

  uint64_t serializedSize() const;

  uint64_t getMemoryUsage() const;

  level_t getHeight() const;

  level_t getSparseStartLevel() const;

  char *serialize() const {
    uint64_t size = serializedSize();
    char *data = new char[size];
    char *cur_data = data;
    louds_dense_->serialize(cur_data);
    louds_sparse_->serialize(cur_data);
    assert(cur_data - data == (int64_t) size);
    return data;
  }

  static FST *deSerialize(char *src) {
    FST *surf = new FST();
    surf->louds_dense_ = LoudsDense::deSerialize(src);
    surf->louds_sparse_ = LoudsSparse::deSerialize(src);
    surf->iter_ = FST::Iter(surf);
    return surf;
  }

 private:
  std::vector<std::string> keys_;
  std::unique_ptr<LoudsSparse> louds_sparse_;
  std::unique_ptr<FSTBuilder> builder_;
  std::unique_ptr<LoudsDense> louds_dense_;

  FST::Iter iter_;
  FST::Iter end_;
};

void FST::create(const std::vector<std::string> &keys, const std::vector<uint64_t> &values, const bool include_dense,
                 const uint32_t sparse_dense_ratio) {
  builder_ = std::make_unique<FSTBuilder>(include_dense, sparse_dense_ratio);
  builder_->build(keys, values);
  louds_dense_ = std::make_unique<LoudsDense>(builder_.get(), keys);
  louds_sparse_ = std::make_unique<LoudsSparse>(builder_.get(), keys);
  iter_ = FST::Iter(this);
  builder_.reset();
}

bool FST::lookupKey(const uint32_t key, uint64_t &value) const {
  // transform uint32 to string
  uint32_t endian_swapped_word = __builtin_bswap32(key);
  std::string transformed_key = std::string(reinterpret_cast<const char *>(&endian_swapped_word), 4);

  position_t connect_node_num = 0;
  if (!louds_dense_->lookupKey(transformed_key, connect_node_num, value))
    return false;
  else if (connect_node_num != 0)
    return louds_sparse_->lookupKey(transformed_key, connect_node_num, value);
  return true;
}

bool FST::lookupKey(const uint64_t key, uint64_t &value) const {
  // transform uint32 to string
  uint64_t endian_swapped_word = __builtin_bswap64(key);
  std::string transformed_key = std::string(reinterpret_cast<const char *>(&endian_swapped_word), 8);

  position_t connect_node_num = 0;
  if (!louds_dense_->lookupKey(transformed_key, connect_node_num, value))
    return false;
  else if (connect_node_num != 0)
    return louds_sparse_->lookupKey(transformed_key, connect_node_num, value);
  return true;
}

bool FST::lookupKey(const std::string &key, uint64_t &value) const {
  position_t connect_node_num = 0;
  if (!louds_dense_->lookupKey(key, connect_node_num, value))
    return false;
  else if (connect_node_num != 0)
    return louds_sparse_->lookupKey(key, connect_node_num, value);
  return true;
}

uint64_t FST::lookupNodeNum(const char *key, uint64_t key_length) const {
  position_t node_num = 0;
  if (louds_dense_->lookupNodeNumber(key, key_length, node_num))
    if (key_length >= louds_sparse_->getStartLevel()) louds_sparse_->lookupNodeNumber(key, key_length, node_num);
  return node_num;
}

std::pair<bool, uint64_t> FST::lookupNodeNumOption(const char *key, uint64_t key_length) const {
  position_t node_num = 0;
  if (!louds_dense_->lookupNodeNumberOption(key, key_length, node_num)) return {false, UINT64_MAX};
  if (key_length < louds_sparse_->getStartLevel()) return {false, UINT64_MAX};
  if (louds_sparse_->lookupNodeNumberOption(key, key_length, node_num)) return {true, node_num};
  return {false, UINT64_MAX};
}

inline bool FST::lookupKeyAtNode(const char *key, uint64_t key_length, level_t level, size_t node_number,
                                 uint64_t &value) const {
  if (level < getSparseStartLevel()) {  // start lookup in LoudsDense
    if (!louds_dense_->lookupKeyAtNode(key, key_length, level, node_number, value)) {
      return false;  // key not immanent in LoudsDense
    } else if (node_number != 0) {
      // continue lookup in LoudsSparse at sparse startlevel
      return louds_sparse_->lookupKeyAtNode(key, key_length, node_number, value, getSparseStartLevel());
    }
    return true;
  }
  // start lookup in LoudsSparse at level and nodenumber
  return louds_sparse_->lookupKeyAtNode(key, key_length, node_number, value, level);
}

// store result in node_number when it gets found
inline bool FST::amacLookup(const char keyByte, level_t level, size_t &node_number) const {
  if (level < getSparseStartLevel()) {  // lookup in LoudsDense
    return louds_dense_->findNextNodeOrValue(keyByte, node_number);

  } else {  // lookup in LoudsSparse
    return louds_sparse_->findNextNodeOrValue(keyByte, node_number);
  }
}

/// For the given node_number, this function returns the first node that is a
/// leaf node or has at least two branches
/// It recursively goes down if a node has only one label and stores these
/// in prefixLabels
inline void FST::getNode(level_t level, size_t node_number, std::vector<uint8_t> &lables, std::vector<uint64_t> &values,
                         std::vector<uint8_t> &prefixLabels, std::vector<uint64_t> &fst_node_numbers) const {
  while (level < getSparseStartLevel() &&
      !louds_dense_->nodeHasMultipleBranchesOrTerminates(node_number, level, prefixLabels)) {
    fst_node_numbers.emplace_back(node_number);
    level++;
  }
  if (level < getSparseStartLevel()) {  // get node from louds_dense_
    louds_dense_->getNode(node_number, lables, values);
  } else {  // continue traversing in louds_sparse_ until node is found that is a leaf or that has at least two labels
    while (!louds_sparse_->nodeHasMultipleBranchesOrTerminates(node_number, level, prefixLabels)) {
      fst_node_numbers.emplace_back(node_number);
      level++;
    }
    // get node from louds_sparse_
    louds_sparse_->getNode(node_number, lables, values);
  }
}

void FST::moveToLeftmostKeyStartingAtNode(level_t level, size_t node_number, FST::Iter& iter) const {

  if (level < getSparseStartLevel()) { // starting in dense part
    iter.dense_iter_.setToFirstLabelInNode(node_number, level);
    iter.dense_iter_.moveToLeftMostKey();

    assert (iter.dense_iter_.isValid());
    if (iter.dense_iter_.isComplete()) return;

    // todo what does isSearchComplete mean here for dense iterator?

    // hand over to sparse iterator
    if (!iter.dense_iter_.isMoveLeftComplete()) {
      iter.passToSparse();
      iter.sparse_iter_.moveToLeftMostKey();
      return;
    }
  } else { // directly start in sparse levels
    iter.dense_iter_.skip(); // skip the dense levels
    iter.sparse_iter_.setStartNodeNum(node_number);
    iter.sparse_iter_.moveToLeftMostKey();
  }
};

FST::Iter FST::moveToKeyStartingAtNode(level_t &level,
                                       size_t node_number,
                                       const std::string &key) const {
  FST::Iter iter(this);

  if (level < getSparseStartLevel()) { // starting in dense part
    // handle dense levels
    louds_dense_->moveToKeyGreaterThanStartingNodeNumber(node_number, level, key, true, iter.dense_iter_);
    if (!iter.dense_iter_.isValid()) return iter;
    if (iter.dense_iter_.isComplete()) return iter;
    // handle sparse levels
    if (!iter.dense_iter_.isSearchComplete()) {
      iter.passToSparse();
      louds_sparse_->moveToKeyGreaterThan(key, true, level, iter.sparse_iter_);
      if (!iter.sparse_iter_.isValid()) iter.incrementDenseIter();
      return iter;
    } else if (!iter.dense_iter_.isMoveLeftComplete()) {
      iter.passToSparse();
      iter.sparse_iter_.moveToLeftMostKey();
      return iter;
    }
  } else { // directly start in sparse levels
    iter.dense_iter_.skip(); // skip the dense levels
    iter.sparse_iter_.setStartNodeNum(node_number);
    louds_sparse_->moveToKeyGreaterThan(key, true, level, iter.sparse_iter_);
    if (!iter.sparse_iter_.isValid()) iter.incrementDenseIter();
    return iter;
  }
  throw;  // shouldn't reach here
};

FST::Iter FST::moveToKeyGreaterThan(const std::string &key, const bool inclusive) const {
  FST::Iter iter(this);
  // todo do not move iterator,
  louds_dense_->moveToKeyGreaterThan(key, inclusive, iter.dense_iter_);

  if (!iter.dense_iter_.isValid()) return iter;
  if (iter.dense_iter_.isComplete()) return iter;

  if (!iter.dense_iter_.isSearchComplete()) {
    iter.passToSparse();
    louds_sparse_->moveToKeyGreaterThan(key, inclusive, iter.sparse_iter_);
    if (!iter.sparse_iter_.isValid()) iter.incrementDenseIter();
    return iter;
  } else if (!iter.dense_iter_.isMoveLeftComplete()) {
    iter.passToSparse();
    iter.sparse_iter_.moveToLeftMostKey();
    return iter;
  }

  assert(false);  // shouldn't reach here
  return iter;
}

FST::Iter FST::moveToKeyLessThan(const std::string &key, const bool inclusive) const {
  FST::Iter iter = moveToKeyGreaterThan(key, false);
  if (!iter.isValid()) {
    iter = moveToLast();
    return iter;
  }
  // if (!iter.getFpFlag()) {
  //  iter--;
  //  uint64_t value = 0;
  //  if (lookupKey(key, value)) iter--;
  //}
  return iter;
}

FST::Iter FST::moveToFirst() const {
  FST::Iter iter(this);
  if (louds_dense_->getHeight() > 0) {
    iter.dense_iter_.setToFirstLabelInRoot();
    iter.dense_iter_.moveToLeftMostKey();
    if (iter.dense_iter_.isMoveLeftComplete()) return iter;
    iter.passToSparse();
    iter.sparse_iter_.moveToLeftMostKey();
  } else {
    iter.sparse_iter_.setToFirstLabelInRoot();
    iter.sparse_iter_.moveToLeftMostKey();
  }
  return iter;
}

FST::Iter FST::moveToLast() const {
  FST::Iter iter(this);
  if (louds_dense_->getHeight() > 0) {
    iter.dense_iter_.setToLastLabelInRoot();
    iter.dense_iter_.moveToRightMostKey();
    if (iter.dense_iter_.isMoveRightComplete()) return iter;
    iter.passToSparse();
    iter.sparse_iter_.moveToRightMostKey();
  } else {
    iter.sparse_iter_.setToLastLabelInRoot();
    iter.sparse_iter_.moveToRightMostKey();
  }
  return iter;
}

std::pair<FST::Iter, FST::Iter> FST::lookupRange(const std::string &left_key, const bool left_inclusive,
                                                 const std::string &right_key, const bool right_inclusive) {
  auto begin_iter = moveToKeyGreaterThan(left_key, left_inclusive);
  auto end_iter = moveToKeyGreaterThan(right_key, true);

  // the right key should be inclusive -> move end-iterator to next element if
  // there exists one
  if (right_inclusive) {
    if (end_iter.isValid()) {
      // move the iterator only in the case that right_key has been found
      // auto tid = end_iter.getValue();
      // todo there is no reference to keys vector anymore
      // if ((*keys_)[tid] == right_key) {
      //  end_iter++;
      //}
    }
  }

  if (end_iter.isValid() && begin_iter.isValid() && begin_iter.getKey() > end_iter.getKey()) {
    return {Iter(), Iter()};
  }

  return {begin_iter, end_iter};
}

uint64_t FST::serializedSize() const { return (louds_dense_->serializedSize() + louds_sparse_->serializedSize()); }

uint64_t FST::getMemoryUsage() const {
  return (sizeof(FST) + louds_dense_->getMemoryUsage() + louds_sparse_->getMemoryUsage());
}

level_t FST::getHeight() const { return louds_sparse_->getHeight(); }

level_t FST::getSparseStartLevel() const { return louds_sparse_->getStartLevel(); }

//============================================================================

void FST::Iter::clear() {
  dense_iter_.clear();
  sparse_iter_.clear();
}

bool FST::Iter::isValid() const {
  return dense_iter_.isValid() && (dense_iter_.isComplete() || sparse_iter_.isValid());
}

int FST::Iter::compare(const std::string &key) const {
  assert(isValid());
  int dense_compare = dense_iter_.compare(key);
  if (dense_iter_.isComplete() || dense_compare != 0) return dense_compare;
  return sparse_iter_.compare(key);
}

uint64_t FST::Iter::getValue() const {
  if (dense_iter_.isComplete()) return dense_iter_.getValue();
  return sparse_iter_.getValue();
}

std::string FST::Iter::getKey() const {
  if (!isValid()) return std::string();
  if (dense_iter_.isComplete()) return dense_iter_.getKey();
  return dense_iter_.getKey() + sparse_iter_.getKey();
}

void FST::Iter::passToSparse() { sparse_iter_.setStartNodeNum(dense_iter_.getSendOutNodeNum()); }

bool FST::Iter::incrementDenseIter() {
  if (!dense_iter_.isValid() || dense_iter_.isSkipped()) return false;

  dense_iter_++;
  if (!dense_iter_.isValid()) return false;
  if (dense_iter_.isMoveLeftComplete()) return true;

  passToSparse();
  sparse_iter_.moveToLeftMostKey();
  return true;
}

bool FST::Iter::incrementSparseIter() {
  if (!sparse_iter_.isValid()) return false;
  sparse_iter_++;
  return sparse_iter_.isValid();
}

bool FST::Iter::operator++(int) {
  if (!isValid()) return false;
  if (incrementSparseIter()) return true;
  return incrementDenseIter();
}

bool FST::Iter::decrementDenseIter() {
  if (!dense_iter_.isValid()) return false;

  dense_iter_--;
  if (!dense_iter_.isValid()) return false;
  if (dense_iter_.isMoveRightComplete()) return true;

  passToSparse();
  sparse_iter_.moveToRightMostKey();
  return true;
}

bool FST::Iter::decrementSparseIter() {
  if (!sparse_iter_.isValid()) return false;
  sparse_iter_--;
  return sparse_iter_.isValid();
}

bool FST::Iter::operator--(int) {
  if (!isValid()) return false;
  if (decrementSparseIter()) return true;
  return decrementDenseIter();
}

bool FST::Iter::operator!=(const FST::Iter &other) {
  // compare two iterators

  // both iterators invalid
  if (!this->isValid() && !other.isValid()) {
    return false;
  }

  // one of them is invalid -> iterators are not equal
  if (!this->isValid() || !other.isValid()) {
    return true;
  }

  // compare dense iterator only if both are not skipped
  if (!this->dense_iter_.isSkipped() && !other.dense_iter_.isSkipped()) {
    if (this->dense_iter_.getLastIteratorPosition() != other.dense_iter_.getLastIteratorPosition()) {
      return true;
    }

    // dense iterators are equal and both of them are complete -> search not
    // continued in sparse levels
    if (this->dense_iter_.isComplete() && other.dense_iter_.isComplete()) {
      return false;
    }
  }

  // dense is equal, check sparse levels now
  return this->sparse_iter_.getLastIteratorPosition() != other.sparse_iter_.getLastIteratorPosition();
}
}  // namespace fst

#endif  // SURF_H
