#ifndef LOUDSDENSE_H_
#define LOUDSDENSE_H_

#include <string>

#include "config.hpp"
#include "fst_builder.hpp"
#include "rank.hpp"

namespace fst {

class LoudsDense {
 public:
  class Iter {
   public:
    Iter()
        : is_valid_(false),
          is_search_complete_(false),
          is_move_left_complete_(false),
          is_move_right_complete_(false),
          send_out_node_num_(0),
          key_len_(0),
          is_at_prefix_key_(false),
          is_skipped_(false),
          skipped_ht_levels_(0) {
      trie_ = nullptr;
    };

    explicit Iter(LoudsDense *trie)
        : is_valid_(false),
          is_search_complete_(false),
          is_move_left_complete_(false),
          is_move_right_complete_(false),
          trie_(trie),
          send_out_node_num_(0),
          key_len_(0),
          is_at_prefix_key_(false),
          is_skipped_(false),
          skipped_ht_levels_(0) {

        const auto height = trie_->getHeight();
        key_.resize(height, 0);
        pos_in_trie_.resize(height, 0);
        value_pos_.resize(height, 0);
        value_pos_initialized_.resize(height, false);
    }

    void clear();

    // hybrid trie might skip dense encoding and directly enter sparse levels
    void skip() { is_skipped_ = true; }

    bool isSkipped() const { return is_skipped_; }

    bool isValid() const { return is_valid_ || is_skipped_; };

    bool isSearchComplete() const { return is_search_complete_; };

    bool isMoveLeftComplete() const { return is_move_left_complete_; };

    bool isMoveRightComplete() const { return is_move_right_complete_; };

    bool isComplete() const {
      return (is_search_complete_ &&
          (is_move_left_complete_ && is_move_right_complete_));
    }

    int compare(const std::string &key) const;

    std::string getKey() const;

    position_t getSendOutNodeNum() const { return send_out_node_num_; };

    void setToFirstLabelInNode(size_t node_number, level_t skipped_ht_levels);

    void setToFirstLabelInRoot();

    void setToLastLabelInRoot();

    void moveToLeftMostKey();

    void moveToRightMostKey();

    uint64_t getLastIteratorPosition() const;

    uint64_t getValue() const;

    void rankValuePosition(size_t pos);

    void operator++(int);

    void operator--(int);

   private:
    inline void append(position_t pos);

    inline void set(level_t level, position_t pos);

    inline void setSendOutNodeNum(position_t node_num) {
      send_out_node_num_ = node_num;
    };

    inline void setFlags(bool is_valid, bool is_search_complete,
                         bool is_move_left_complete,
                         bool is_move_right_complete);

   private:
    // True means the iter either points to a valid key
    // or to a prefix with length trie_->getHeight()
    bool is_valid_;
    // If false, call moveToKeyGreaterThan in LoudsSparse to complete
    bool is_search_complete_;
    // If false, call moveToLeftMostKey in LoudsSparse to complete
    bool is_move_left_complete_;
    // If false, call moveToRightMostKey in LoudsSparse to complete
    bool is_move_right_complete_;
    LoudsDense *trie_;
    position_t send_out_node_num_;
    level_t key_len_;  // Does NOT include suffix
    level_t skipped_ht_levels_;

    std::vector<label_t> key_;
    std::vector<position_t> pos_in_trie_;

    // stores the index of the current (sparse) value
    std::vector<position_t> value_pos_;
    std::vector<bool> value_pos_initialized_;
    bool is_at_prefix_key_;
    bool is_skipped_; // hybrid trie might skip the dense encoding

    friend class LoudsDense;
  };

 public:
  LoudsDense() = default;

  LoudsDense(FSTBuilder *builder,
             const std::vector<std::string> &keys);

  ~LoudsDense() = default;

  // Returns whether key exists in the trie so far
  // out_node_num == 0 means search terminates in louds-dense.
  bool lookupKey(const std::string &key, position_t &out_node_num,
                 uint64_t &value) const;

  // this function checks if the FST node has only one branch
  bool nodeHasMultipleBranchesOrTerminates(size_t &nodeNumber, size_t level, std::vector<uint8_t> &prefixLabels) const;

  // this function stores the entire node for the given nodeNumber in labels and
  // values vectors
  void getNode(size_t nodeNumber, std::vector<uint8_t> &labels, std::vector<uint64_t> &values);

  bool lookupKeyAtNode(const char *key, uint64_t key_length, level_t level, size_t &node_number, uint64_t &value) const;

  bool lookupNodeNumber(const char *key, uint64_t key_length, position_t &out_node_num) const;

  bool lookupNodeNumberOption(const char *key, uint64_t key_length, position_t &out_node_num) const;

  bool findNextNodeOrValue(const char keyByte, size_t &node_number) const;

  void moveToKeyGreaterThanStartingNodeNumber(position_t nodeNumber,
                                              level_t &level,
                                              const std::string &searched_key,
                                              bool inclusive,
                                              LoudsDense::Iter &iter) const;

  // return value indicates potential false positive
  void moveToKeyGreaterThan(const std::string &searched_key, bool inclusive,
                            LoudsDense::Iter &iter) const;

  uint64_t getHeight() const { return height_; };

  uint64_t serializedSize() const;

  uint64_t getMemoryUsage() const;

  void serialize(char *&dst) const {
    memcpy(dst, &height_, sizeof(height_));
    dst += sizeof(height_);
    align(dst);
    label_bitmaps_->serialize(dst);
    child_indicator_bitmaps_->serialize(dst);
    prefixkey_indicator_bits_->serialize(dst);
    align(dst);
  }

  static std::unique_ptr<LoudsDense> deSerialize(char *&src) {
    std::unique_ptr<LoudsDense> louds_dense = std::make_unique<LoudsDense>();
    memcpy(&(louds_dense->height_), src, sizeof(louds_dense->height_));
    src += sizeof(louds_dense->height_);
    align(src);
    louds_dense->label_bitmaps_ = BitvectorRank::deSerialize(src);
    louds_dense->child_indicator_bitmaps_ = BitvectorRank::deSerialize(src);
    louds_dense->prefixkey_indicator_bits_ = BitvectorRank::deSerialize(src);
    align(src);
    return louds_dense;
  }

 private:
  position_t getChildNodeNum(position_t pos) const;

  position_t getSuffixPos(position_t pos, bool is_prefix_key) const;

  position_t getNextPos(position_t pos) const;

  position_t getPrevPos(position_t pos, bool *is_out_of_bound) const;

 private:
  static const position_t kNodeFanout = 256;
  static const position_t kRankBasicBlockSize = 512;

  std::vector<uint64_t> values_dense_;

  level_t height_{};

  std::unique_ptr<BitvectorRank> label_bitmaps_;
  std::unique_ptr<BitvectorRank> child_indicator_bitmaps_;
  std::unique_ptr<BitvectorRank> prefixkey_indicator_bits_;
  // const pointer to the original keys
  const std::vector<std::string> *keys_{};
};

const position_t LoudsDense::kNodeFanout;
const position_t LoudsDense::kRankBasicBlockSize;

LoudsDense::LoudsDense(FSTBuilder *builder,
                       const std::vector<std::string> &keys) {
  keys_ = &keys;
  height_ = builder->getSparseStartLevel();
  std::vector<position_t> num_bits_per_level;
  for (level_t level = 0; level < height_; level++)
    num_bits_per_level.push_back(builder->getBitmapLabels()[level].size() *
        kWordSize);

  label_bitmaps_ = std::make_unique<BitvectorRank>(kRankBasicBlockSize,
                                                   builder->getBitmapLabels(),
                                                   num_bits_per_level,
                                                   0,
                                                   height_);
  child_indicator_bitmaps_ =
      std::make_unique<BitvectorRank>(kRankBasicBlockSize,
                                      builder->getBitmapChildIndicatorBits(),
                                      num_bits_per_level,
                                      0,
                                      height_);
  prefixkey_indicator_bits_ =
      std::make_unique<BitvectorRank>(kRankBasicBlockSize,
                                      builder->getPrefixkeyIndicatorBits(),
                                      builder->getNodeCounts(),
                                      0,
                                      height_);

  // todo make more efficient by completely moving this vector
  values_dense_ = builder->getDenseValues();
}

bool LoudsDense::lookupKey(const std::string &key, position_t &out_node_num,
                           uint64_t &value) const {
  position_t node_num = 0;
  position_t pos = 0;
  for (level_t level = 0; level < height_; level++) {
    pos = (node_num * kNodeFanout);
    if (level >= key.length()) {  // if run out of searchKey bytes
      return false;
    }
    pos += (label_t) key[level];

    // child_indicator_bitmaps_->prefetch(pos);

    if (!label_bitmaps_->readBit(pos)) {  // if key byte does not exist
      return false;
    }

    if (!child_indicator_bitmaps_->readBit(pos)) {  // if trie branch terminates
      uint64_t value_index = label_bitmaps_->rank(pos) -
          child_indicator_bitmaps_->rank(pos) -
          1;  // + prefix but we do not support this so far
      value = values_dense_[value_index];

      // the following check must be performed by the caller
      // return (*keys_)[value] == key;
      return true;
    }
    node_num = getChildNodeNum(pos);
  }
  // search will continue in LoudsSparse
  out_node_num = node_num;
  return true;
}

inline bool LoudsDense::lookupKeyAtNode(const char *key,
                                        uint64_t key_length,
                                        level_t level,
                                        size_t &node_num,
                                        uint64_t &value) const {
  position_t pos = 0;
  for (; level < height_; level++) {
    pos = (node_num * kNodeFanout);
    if (level >= key_length) {  // if run out of searchKey bytes
      return false;
    }
    pos += (label_t) key[level];

    if (!label_bitmaps_->readBit(pos)) {  // if key byte does not exist
      return false;
    }

    if (!child_indicator_bitmaps_->readBit(pos)) {  // if trie branch terminates
      uint64_t value_index = label_bitmaps_->rank(pos) -
          child_indicator_bitmaps_->rank(pos) -
          1;  // + prefix but we do not support this so far
      value = values_dense_[value_index];

      // the following check must be performed by the caller
      // return (*keys_)[value] == key;
      node_num = 0;
      return true;
    }
    node_num = getChildNodeNum(pos);
  }
  // search will continue in LoudsSparse
  return true;
}

/// Returns true if one or two of the following conditions evaluate to true:
/// 1. Node has at least two labels
/// 2. If has one label that leads not to a child node
bool LoudsDense::nodeHasMultipleBranchesOrTerminates(size_t &nodeNumber,
                                                     size_t level,
                                                     std::vector<uint8_t> &prefixLabels) const {
  unsigned label = 0;
  assert(label_bitmaps_->getNumSetBitsInDenseNode(nodeNumber, label) > 0);
  if (label_bitmaps_->getNumSetBitsInDenseNode(nodeNumber, label) == 1) {
    // node has only one label
    position_t pos = (nodeNumber * kNodeFanout) + label;
    if (!child_indicator_bitmaps_->readBit(pos)) // branch terminates
      return true;
    prefixLabels.emplace_back(label);
    nodeNumber = getChildNodeNum(pos);
    return false;
  } else { // there are at least two labels in the node
    return true;
  }
}

void LoudsDense::getNode(size_t nodeNumber, std::vector<uint8_t> &labels, std::vector<uint64_t> &values) {
  position_t pos = (nodeNumber * kNodeFanout);
  label_bitmaps_->prefetch(pos);
  child_indicator_bitmaps_->prefetch(pos);
  for (size_t i = 0; i < 256; i++) {
    if (label_bitmaps_->readBit(pos + i)) {
      labels.push_back(i);
      if (child_indicator_bitmaps_->readBit(pos + i)) { // label leads to child node
        // inline information in value that it is a FST node Number
        values.emplace_back(getChildNodeNum(pos + i) << 2U | 3U);
      } else {
        // there is a value, push it back and create an ART leaf node
        uint64_t value_index = label_bitmaps_->rank(pos + i) - child_indicator_bitmaps_->rank(pos + i) - 1;
        auto value = values_dense_[value_index];
        values.emplace_back((value << 2U) | 1U);
      }
    }
  }

}

bool LoudsDense::lookupNodeNumber(const char *key, uint64_t key_length, position_t &out_node_num) const {
  position_t node_num = 0;
  position_t pos = 0;
  level_t level = 0;
  // todo check when to return true but set node_num to 0 -> finished in dense already
  for (; level < height_ && level < key_length; level++) {
    pos = (node_num * kNodeFanout);
    /*if (level >= key_length) {  // if run out of searchKey bytes
      out_node_num = node_num;
      return false;
    }*/
    pos += reinterpret_cast<const uint8_t *>(key)[level];

    assert(label_bitmaps_->readBit(pos)); // assert that key exists
    assert(child_indicator_bitmaps_->readBit(pos)); // assert branch does not terminate

    node_num = getChildNodeNum(pos);
  }

  // search will continue in LoudsSparse
  out_node_num = node_num;
  return true;
}

bool LoudsDense::lookupNodeNumberOption(const char *key, uint64_t key_length, position_t &out_node_num) const {
  position_t node_num = 0;
  position_t pos = 0;
  level_t level = 0;
  // todo check when to return true but set node_num to 0 -> finished in dense already
  for (; level < height_ && level < key_length; level++) {
    pos = (node_num * kNodeFanout);
    if (level >= key_length) {  // if run out of searchKey bytes
      out_node_num = node_num;
      return false;
    }
    pos += reinterpret_cast<const uint8_t *>(key)[level];

    if (!label_bitmaps_->readBit(pos)) return false; // does key exists?
    if (!child_indicator_bitmaps_->readBit(pos)) return false; // does branch not terminate?

    node_num = getChildNodeNum(pos);
  }

  // search will continue in LoudsSparse
  out_node_num = node_num;
  return true;
}

// returns true if next node or value is found, false if keyByte is not immanent
// 1. next nodenumber has been found, return true
//  - in this case, return next nodenumber and set last to bits to 01
// 2. result has been found, return true
//  - in this case, return result and set the last to bits to 11
// 3. keyByte does not exist in given node
//  - return false
bool LoudsDense::findNextNodeOrValue(const char keyByte, size_t &node_number) const {
  position_t pos = (node_number * kNodeFanout) + keyByte;
  if (!label_bitmaps_->readBit(pos)) { // key not immanent
    return false;
  }
  // key exists
  if (!child_indicator_bitmaps_->readBit(pos)) { // branch terminates
    uint64_t value_index =
        label_bitmaps_->rank(pos) -
            child_indicator_bitmaps_->rank(pos) - 1;
    node_number = (values_dense_[value_index] << 2u) | 1u;
  } else { // branch continues
    node_number = (getChildNodeNum(pos) << 2u) | 3u;
  }
  return true;
}

void LoudsDense::moveToKeyGreaterThanStartingNodeNumber(position_t node_num,
                                                        level_t &level,
                                                        const std::string &searched_key,
                                                        bool inclusive,
                                                        LoudsDense::Iter &iter) const {
  iter.skipped_ht_levels_ = level;
  position_t pos;
  for (; level < height_; level++) {
    // if is_at_prefix_key_, pos is at the next valid position in the child node
    pos = node_num * kNodeFanout;
    if (level >= searched_key.length()) {  // if run out of searchKey bytes
      // CA: key too short, -> dense (& sparse) traverse to leftmost key a
      iter.append(getNextPos(pos - 1));
      iter.moveToLeftMostKey();
      // valid, search complete, moveLeft complete, moveRight complete
      //iter.setFlags(true, true, true, true);
      return;
    }

    pos += (label_t) searched_key[level];
    iter.append(pos);

    // if no exact match
    if (!label_bitmaps_->readBit(pos)) {
      iter.moveToLeftMostKey(); // search could continue in sparse levels
      return;
    }

    // if trie branch terminates
    if (!child_indicator_bitmaps_->readBit(pos)) {
      iter.rankValuePosition(pos);
      auto found_key = (*keys_)[iter.getValue()];

      if (found_key > searched_key) {
        iter.setFlags(true, true, true, true);
      } else if (found_key < searched_key) {
        iter++; // no exact match, inclusive flag is not relevant
      } else { // found_key == searched_key
        if (!inclusive)
          iter++;
        else
          iter.setFlags(true, true, true, true);
      }
      // set value index here
      //iter.rankValuePosition(pos);
      return;
    }
    node_num = getChildNodeNum(pos);
  }

  // search will continue in LoudsSparse
  iter.setSendOutNodeNum(node_num);
  // valid, search INCOMPLETE, moveLeft complete, moveRight complete
  iter.setFlags(true, false, true, true);
}

void LoudsDense::moveToKeyGreaterThan(const std::string &searched_key,
                                      const bool inclusive,
                                      LoudsDense::Iter &iter) const {
  position_t node_num = 0;
  position_t pos = 0;
  for (level_t level = 0; level < height_; level++) {
    // if is_at_prefix_key_, pos is at the next valid position in the child node
    pos = node_num * kNodeFanout;
    if (level >= searched_key.length()) {  // if run out of searchKey bytes
      // CA: key too short, -> dense (& sparse) traverse to leftmost key a
      iter.append(getNextPos(pos - 1));
      //if (prefixkey_indicator_bits_->readBit(node_num))  // if the prefix is also a key
      //  iter.is_at_prefix_key_ = true;
      //else
      iter.moveToLeftMostKey();
      // valid, search complete, moveLeft complete, moveRight complete
      //iter.setFlags(true, true, true, true);
      return;
    }

    pos += (label_t) searched_key[level];
    iter.append(pos);

    // if no exact match
    if (!label_bitmaps_->readBit(pos)) {
      iter.moveToLeftMostKey(); // search could continue in sparse levels
      return;
    }

    // if trie branch terminates
    if (!child_indicator_bitmaps_->readBit(pos)) {
      iter.rankValuePosition(pos);
      auto found_key = (*keys_)[iter.getValue()];

      if (found_key > searched_key) {
        iter.setFlags(true, true, true, true);
      } else if (found_key < searched_key) {
        iter++; // no exact match, inclusive flag is not relevant
      } else { // found_key == searched_key
        if (!inclusive)
          iter++;
        else
          iter.setFlags(true, true, true, true);
      }
      // set value index here
      //iter.rankValuePosition(pos);
      return;
    }
    node_num = getChildNodeNum(pos);
  }

  // search will continue in LoudsSparse
  iter.setSendOutNodeNum(node_num);
  // valid, search INCOMPLETE, moveLeft complete, moveRight complete
  iter.setFlags(true, false, true, true);
}

uint64_t LoudsDense::serializedSize() const {
  uint64_t size = sizeof(height_) + label_bitmaps_->serializedSize() +
      child_indicator_bitmaps_->serializedSize() +
      prefixkey_indicator_bits_->serializedSize();
  sizeAlign(size);
  return size;
}

uint64_t LoudsDense::getMemoryUsage() const {
  return (sizeof(LoudsDense) + label_bitmaps_->size() +
      child_indicator_bitmaps_->size() + prefixkey_indicator_bits_->size()
      + values_dense_.size() * 8);
}

position_t LoudsDense::getChildNodeNum(const position_t pos) const {
  return child_indicator_bitmaps_->rank(pos);
}

position_t LoudsDense::getSuffixPos(const position_t pos,
                                    const bool is_prefix_key) const {
  position_t node_num = pos / kNodeFanout;
  position_t suffix_pos =
      (label_bitmaps_->rank(pos) - child_indicator_bitmaps_->rank(pos) +
          prefixkey_indicator_bits_->rank(node_num) - 1);
  if (is_prefix_key && label_bitmaps_->readBit(pos) &&
      !child_indicator_bitmaps_->readBit(pos))
    suffix_pos--;
  return suffix_pos;
}

position_t LoudsDense::getNextPos(const position_t pos) const {
  return pos + label_bitmaps_->distanceToNextSetBit(pos);
}

position_t LoudsDense::getPrevPos(const position_t pos,
                                  bool *is_out_of_bound) const {
  position_t distance = label_bitmaps_->distanceToPrevSetBit(pos);
  if (pos <= distance) {
    *is_out_of_bound = true;
    return 0;
  }
  *is_out_of_bound = false;
  return (pos - distance);
}

//============================================================================

void LoudsDense::Iter::clear() {
  is_valid_ = false;
  is_search_complete_ = false;
  is_move_left_complete_ = false;
  is_move_right_complete_ = false;
  send_out_node_num_ = 0;
  key_len_ = 0;
  is_at_prefix_key_ = false;
  is_skipped_ = false;
  skipped_ht_levels_ = 0;

  std::fill(key_.begin(), key_.end(), 0);
  std::fill(pos_in_trie_.begin(), pos_in_trie_.end(), 0);
  std::fill(value_pos_.begin(), value_pos_.end(), 0);
  std::fill(value_pos_initialized_.begin(), value_pos_initialized_.end(), false);

}

int LoudsDense::Iter::compare(const std::string &key) const {
  if (is_at_prefix_key_ && (key_len_ - 1) < key.length()) return -1;
  std::string iter_key = getKey();
  std::string key_dense = key.substr(0, iter_key.length());
  int compare = iter_key.compare(key_dense);
  if (compare != 0) return compare;
  return compare;
}

std::string LoudsDense::Iter::getKey() const {
  if (!is_valid_) return std::string();
  level_t len = key_len_;
  if (is_at_prefix_key_) len--;
  return std::string((const char *) key_.data(), (size_t) len);
}

void LoudsDense::Iter::append(position_t pos) {
  assert(key_len_ < key_.size());
  key_[key_len_] = (label_t) (pos % kNodeFanout);
  pos_in_trie_[key_len_] = pos;
  key_len_++;
}

void LoudsDense::Iter::set(level_t level, position_t pos) {
  assert(level < key_.size());
  key_[level] = (label_t) (pos % kNodeFanout);
  pos_in_trie_[level] = pos;
}

void LoudsDense::Iter::setFlags(const bool is_valid,
                                const bool is_search_complete,
                                const bool is_move_left_complete,
                                const bool is_move_right_complete) {
  is_valid_ = is_valid;
  is_search_complete_ = is_search_complete;
  is_move_left_complete_ = is_move_left_complete;
  is_move_right_complete_ = is_move_right_complete;
}

// skipped_ht_levels stores the level of the current node number
void LoudsDense::Iter::setToFirstLabelInNode(size_t node_number, level_t skipped_ht_levels) {
  skipped_ht_levels_ = skipped_ht_levels;
  position_t pos = node_number * kNodeFanout; // at first position in dense node
  if (trie_->label_bitmaps_->readBit(pos)) {
    pos_in_trie_[0] = pos;
    key_[0] = (label_t) (pos % kNodeFanout);
  } else {
    pos_in_trie_[0] = trie_->getNextPos(pos);
    key_[0] = (label_t) (pos_in_trie_[0] % kNodeFanout);
  }
  key_len_++;
};

void LoudsDense::Iter::setToFirstLabelInRoot() {
  if (trie_->label_bitmaps_->readBit(0)) {
    pos_in_trie_[0] = 0;
    key_[0] = (label_t) 0;
  } else {
    pos_in_trie_[0] = trie_->getNextPos(0);
    key_[0] = (label_t) pos_in_trie_[0];
  }
  key_len_++;
}

void LoudsDense::Iter::setToLastLabelInRoot() {
  bool is_out_of_bound;
  pos_in_trie_[0] = trie_->getPrevPos(kNodeFanout, &is_out_of_bound);
  key_[0] = (label_t) pos_in_trie_[0];
  key_len_++;
}

void LoudsDense::Iter::moveToLeftMostKey() {
  assert(key_len_ > 0);
  level_t level = key_len_ - 1;
  position_t pos = pos_in_trie_[level];
  if (!trie_->child_indicator_bitmaps_->readBit(pos)) { // found leaf node, no subtree
    rankValuePosition(pos);
    // valid, search complete, moveLeft complete, moveRight complete
    return setFlags(true, true, true, true);
  }

  while (level < trie_->getHeight() - 1 - skipped_ht_levels_) {
    position_t node_num = trie_->getChildNodeNum(pos);
    // if the current prefix is also a key
    if (trie_->prefixkey_indicator_bits_->readBit(node_num)) {
      append(trie_->getNextPos(node_num * kNodeFanout - 1));
      is_at_prefix_key_ = true;
      // valid, search complete, moveLeft complete, moveRight complete
      return setFlags(true, true, true, true);
    }

    pos = trie_->getNextPos(node_num * kNodeFanout - 1);
    append(pos);

    // if trie branch terminates
    if (!trie_->child_indicator_bitmaps_->readBit(pos)) {
      rankValuePosition(pos);
      // valid, search complete, moveLeft complete, moveRight complete
      return setFlags(true, true, true, true);
    }

    level++;
  }
  send_out_node_num_ = trie_->getChildNodeNum(pos);
  // valid, search complete, moveLeft INCOMPLETE, moveRight complete
  setFlags(true, true, false, true);
}

void LoudsDense::Iter::moveToRightMostKey() {
  assert(key_len_ > 0);
  level_t level = key_len_ - 1;
  position_t pos = pos_in_trie_[level];
  if (!trie_->child_indicator_bitmaps_->readBit(pos))
    // valid, search complete, moveLeft complete, moveRight complete
    return setFlags(true, true, true, true);

  while (level < trie_->getHeight() - 1) {
    position_t node_num = trie_->getChildNodeNum(pos);
    bool is_out_of_bound;
    pos = trie_->getPrevPos((node_num + 1) * kNodeFanout, &is_out_of_bound);
    if (is_out_of_bound) {
      is_valid_ = false;
      return;
    }
    append(pos);

    // if trie branch terminates
    if (!trie_->child_indicator_bitmaps_->readBit(pos))
      // valid, search complete, moveLeft complete, moveRight complete
      return setFlags(true, true, true, true);

    level++;
  }
  send_out_node_num_ = trie_->getChildNodeNum(pos);
  // valid, search complete, moveleft complete, moveRight INCOMPLETE
  setFlags(true, true, true, false);
}

uint64_t LoudsDense::Iter::getLastIteratorPosition() const {
  return pos_in_trie_[key_len_ - 1];
}

uint64_t LoudsDense::Iter::getValue() const {
  return trie_->values_dense_[value_pos_[key_len_ - 1]];
}

void LoudsDense::Iter::rankValuePosition(size_t pos) {
  if (value_pos_initialized_[key_len_ - 1]) {
    value_pos_[key_len_ - 1]++;
  } else {  // initially rank value position here
    value_pos_initialized_[key_len_ - 1] = true;
    uint64_t value_index = trie_->label_bitmaps_->rank(pos) -
        trie_->child_indicator_bitmaps_->rank(pos) -
        1;  // + prefix but we do not support this so far
    value_pos_[key_len_ - 1] = value_index;
  }
}

void LoudsDense::Iter::operator++(int) {
  assert(key_len_ > 0);
  if (is_at_prefix_key_) {
    is_at_prefix_key_ = false;
    return moveToLeftMostKey();
  }
  position_t pos = pos_in_trie_[key_len_ - 1];
  position_t next_pos = trie_->getNextPos(pos);
  // if crossing node boundary
  while ((next_pos / kNodeFanout) > (pos / kNodeFanout)) {
    key_len_--;
    if (key_len_ == 0) {
      is_valid_ = false;
      return;
    }
    pos = pos_in_trie_[key_len_ - 1];
    next_pos = trie_->getNextPos(pos);
  }
  set(key_len_ - 1, next_pos);
  return moveToLeftMostKey();
}

void LoudsDense::Iter::operator--(int) {
  assert(key_len_ > 0);
  if (is_at_prefix_key_) {
    is_at_prefix_key_ = false;
    key_len_--;
  }
  position_t pos = pos_in_trie_[key_len_ - 1];
  bool is_out_of_bound;
  position_t prev_pos = trie_->getPrevPos(pos, &is_out_of_bound);
  if (is_out_of_bound) {
    is_valid_ = false;
    return;
  }

  // if crossing node boundary
  while ((prev_pos / kNodeFanout) < (pos / kNodeFanout)) {
    // if the current prefix is also a key
    position_t node_num = pos / kNodeFanout;
    if (trie_->prefixkey_indicator_bits_->readBit(node_num)) {
      is_at_prefix_key_ = true;
      // valid, search complete, moveLeft complete, moveRight complete
      return setFlags(true, true, true, true);
    }

    key_len_--;
    if (key_len_ == 0) {
      is_valid_ = false;
      return;
    }
    pos = pos_in_trie_[key_len_ - 1];
    prev_pos = trie_->getPrevPos(pos, &is_out_of_bound);
    if (is_out_of_bound) {
      is_valid_ = false;
      return;
    }
  }
  set(key_len_ - 1, prev_pos);
  return moveToRightMostKey();
}
}  // namespace fst

#endif  // LOUDSDENSE_H_
