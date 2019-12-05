#ifndef SURF_H_
#define SURF_H_

#include <string>
#include <vector>

#include "config.hpp"
#include "louds_dense.hpp"
#include "louds_sparse.hpp"
#include "surf_builder.hpp"

namespace surf {

    class SuRF {
    public:
        class Iter {
        public:
            Iter() {};

            Iter(const SuRF *filter) {
                dense_iter_ = LoudsDense::Iter(filter->louds_dense_);
                sparse_iter_ = LoudsSparse::Iter(filter->louds_sparse_);
                could_be_fp_ = false;
            }

            void clear();

            bool isValid() const;

            bool getFpFlag() const;

            int compare(const std::string &key) const;

            uint64_t getValue() const;

            std::string getKey() const;

            int getSuffix(word_t *suffix) const;

            std::string getKeyWithSuffix(unsigned *bitlen) const;

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
            bool could_be_fp_;

            friend class SuRF;
        };

    public:
        SuRF() {};

        //------------------------------------------------------------------
        // Input keys must be SORTED
        //------------------------------------------------------------------
        SuRF(const std::vector<std::string> &keys, const std::vector<uint64_t> &values) {
            create(keys, values, kIncludeDense, kSparseDenseRatio);
        }

        SuRF(const std::vector<std::string> &keys, const std::vector<uint64_t> &values,
             const bool include_dense, const uint32_t sparse_dense_ratio) {
            create(keys, values, include_dense, sparse_dense_ratio);
        }

        ~SuRF() {}

        void create(const std::vector<std::string> &keys, const std::vector<uint64_t> &values, bool include_dense,
                    uint32_t sparse_dense_ratio);

        bool lookupKey(const std::string &key, uint64_t &value) const;

        // This function searches in a conservative way: if inclusive is true
        // and the stored key prefix matches key, iter stays at this key prefix.
        SuRF::Iter moveToKeyGreaterThan(const std::string &key, bool inclusive) const;

        SuRF::Iter moveToKeyLessThan(const std::string &key, bool inclusive) const;

        SuRF::Iter moveToFirst() const;

        SuRF::Iter moveToLast() const;

        std::pair<SuRF::Iter, SuRF::Iter> lookupRange(const std::string &left_key, bool left_inclusive,
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

        static SuRF *deSerialize(char *src) {
            SuRF *surf = new SuRF();
            surf->louds_dense_ = LoudsDense::deSerialize(src);
            surf->louds_sparse_ = LoudsSparse::deSerialize(src);
            surf->iter_ = SuRF::Iter(surf);
            return surf;
        }

        void destroy() {
            louds_dense_->destroy();
            louds_sparse_->destroy();
        }

    private:
        LoudsDense *louds_dense_;
        LoudsSparse *louds_sparse_;
        SuRFBuilder *builder_;
        SuRF::Iter iter_;
        SuRF::Iter end_;
    };

    void SuRF::create(const std::vector<std::string> &keys, const std::vector<uint64_t> &values,
                      const bool include_dense, const uint32_t sparse_dense_ratio) {
        // todo where to store the values?
        builder_ = new SuRFBuilder(include_dense, sparse_dense_ratio);
        builder_->build(keys, values);
        louds_dense_ = new LoudsDense(builder_);
        louds_sparse_ = new LoudsSparse(builder_);
        iter_ = SuRF::Iter(this);
        delete builder_;
    }

    bool SuRF::lookupKey(const std::string &key, uint64_t &value) const {
        position_t connect_node_num = 0;
        if (!louds_dense_->lookupKey(key, connect_node_num, value))
            return false;
        else if (connect_node_num != 0)
            return louds_sparse_->lookupKey(key, connect_node_num, value);
        return true;
    }

    SuRF::Iter SuRF::moveToKeyGreaterThan(const std::string &key, const bool inclusive) const {
        SuRF::Iter iter(this);
        iter.could_be_fp_ = louds_dense_->moveToKeyGreaterThan(key, inclusive, iter.dense_iter_);

        if (!iter.dense_iter_.isValid())
            return iter;
        if (iter.dense_iter_.isComplete())
            return iter;

        if (!iter.dense_iter_.isSearchComplete()) {
            iter.passToSparse();
            iter.could_be_fp_ = louds_sparse_->moveToKeyGreaterThan(key, inclusive, iter.sparse_iter_);
            if (!iter.sparse_iter_.isValid())
                iter.incrementDenseIter();
            return iter;
        } else if (!iter.dense_iter_.isMoveLeftComplete()) {
            iter.passToSparse();
            iter.sparse_iter_.moveToLeftMostKey();
            return iter;
        }

        assert(false); // shouldn't reach here
        return iter;
    }

    SuRF::Iter SuRF::moveToKeyLessThan(const std::string &key, const bool inclusive) const {
        SuRF::Iter iter = moveToKeyGreaterThan(key, false);
        if (!iter.isValid()) {
            iter = moveToLast();
            return iter;
        }
        if (!iter.getFpFlag()) {
            iter--;
            uint64_t value = 0;
            if (lookupKey(key, value))
                iter--;
        }
        return iter;
    }

    SuRF::Iter SuRF::moveToFirst() const {
        SuRF::Iter iter(this);
        if (louds_dense_->getHeight() > 0) {
            iter.dense_iter_.setToFirstLabelInRoot();
            iter.dense_iter_.moveToLeftMostKey();
            if (iter.dense_iter_.isMoveLeftComplete())
                return iter;
            iter.passToSparse();
            iter.sparse_iter_.moveToLeftMostKey();
        } else {
            iter.sparse_iter_.setToFirstLabelInRoot();
            iter.sparse_iter_.moveToLeftMostKey();
        }
        return iter;
    }

    SuRF::Iter SuRF::moveToLast() const {
        SuRF::Iter iter(this);
        if (louds_dense_->getHeight() > 0) {
            iter.dense_iter_.setToLastLabelInRoot();
            iter.dense_iter_.moveToRightMostKey();
            if (iter.dense_iter_.isMoveRightComplete())
                return iter;
            iter.passToSparse();
            iter.sparse_iter_.moveToRightMostKey();
        } else {
            iter.sparse_iter_.setToLastLabelInRoot();
            iter.sparse_iter_.moveToRightMostKey();
        }
        return iter;
    }

    std::pair<SuRF::Iter, SuRF::Iter> SuRF::lookupRange(const std::string &left_key, const bool left_inclusive,
                                                        const std::string &right_key, const bool right_inclusive) {

        auto begin_iter = moveToKeyGreaterThan(left_key, left_inclusive);
        auto end_iter = moveToKeyGreaterThan(right_key, true);

        // the right key should be inclusive -> move end-iterator to next element if there is one
        if (right_inclusive) {
            if (end_iter.isValid()) {
                // do move the iterator only in the case the provided right key has been found
                if (end_iter.getKey() == right_key) {
                    end_iter++;
                }
            }
        }

        if (end_iter.isValid() && begin_iter.isValid() && begin_iter.getKey() > end_iter.getKey()) {
            return {Iter(), Iter()};
        }

        return {begin_iter, end_iter};
        /*
        iter_.clear();
        louds_dense_->moveToKeyGreaterThan(left_key, left_inclusive, iter_.dense_iter_);
        if (!iter_.dense_iter_.isValid()) return false;
        if (!iter_.dense_iter_.isComplete()) {
            if (!iter_.dense_iter_.isSearchComplete()) {
                iter_.passToSparse();
                louds_sparse_->moveToKeyGreaterThan(left_key, left_inclusive, iter_.sparse_iter_);
                if (!iter_.sparse_iter_.isValid()) {
                    iter_.incrementDenseIter();
                }
            } else if (!iter_.dense_iter_.isMoveLeftComplete()) {
                iter_.passToSparse();
                iter_.sparse_iter_.moveToLeftMostKey();
            }
        }
        if (!iter_.isValid()) return false;
        int compare = iter_.compare(right_key);
        if (compare == kCouldBePositive)
            return true;
        if (right_inclusive)
            return (compare <= 0);
        else
            return (compare < 0);
        */
    }

    uint64_t SuRF::serializedSize() const {
        return (louds_dense_->serializedSize()
                + louds_sparse_->serializedSize());
    }

    uint64_t SuRF::getMemoryUsage() const {
        return (sizeof(SuRF) + louds_dense_->getMemoryUsage() + louds_sparse_->getMemoryUsage());
    }

    level_t SuRF::getHeight() const {
        return louds_sparse_->getHeight();
    }

    level_t SuRF::getSparseStartLevel() const {
        return louds_sparse_->getStartLevel();
    }

//============================================================================

    void SuRF::Iter::clear() {
        dense_iter_.clear();
        sparse_iter_.clear();
    }

    bool SuRF::Iter::getFpFlag() const {
        return could_be_fp_;
    }

    bool SuRF::Iter::isValid() const {
        return dense_iter_.isValid()
               && (dense_iter_.isComplete() || sparse_iter_.isValid());
    }

    int SuRF::Iter::compare(const std::string &key) const {
        assert(isValid());
        int dense_compare = dense_iter_.compare(key);
        if (dense_iter_.isComplete() || dense_compare != 0)
            return dense_compare;
        return sparse_iter_.compare(key);
    }

    uint64_t SuRF::Iter::getValue() const {
        // todo
        return 0;
    }


    std::string SuRF::Iter::getKey() const {
        if (!isValid())
            return std::string();
        if (dense_iter_.isComplete())
            return dense_iter_.getKey();
        return dense_iter_.getKey() + sparse_iter_.getKey();
    }

    int SuRF::Iter::getSuffix(word_t *suffix) const {
        if (!isValid())
            return 0;
        if (dense_iter_.isComplete())
            return dense_iter_.getSuffix(suffix);
        return sparse_iter_.getSuffix(suffix);
    }

    std::string SuRF::Iter::getKeyWithSuffix(unsigned *bitlen) const {
        *bitlen = 0;
        if (!isValid())
            return std::string();
        if (dense_iter_.isComplete())
            return dense_iter_.getKeyWithSuffix(bitlen);
        return dense_iter_.getKeyWithSuffix(bitlen) + sparse_iter_.getKeyWithSuffix(bitlen);
    }

    void SuRF::Iter::passToSparse() {
        sparse_iter_.setStartNodeNum(dense_iter_.getSendOutNodeNum());
    }

    bool SuRF::Iter::incrementDenseIter() {
        if (!dense_iter_.isValid())
            return false;

        dense_iter_++;
        if (!dense_iter_.isValid())
            return false;
        if (dense_iter_.isMoveLeftComplete())
            return true;

        passToSparse();
        sparse_iter_.moveToLeftMostKey();
        return true;
    }

    bool SuRF::Iter::incrementSparseIter() {
        if (!sparse_iter_.isValid())
            return false;
        sparse_iter_++;
        return sparse_iter_.isValid();
    }

    bool SuRF::Iter::operator++(int) {
        if (!isValid())
            return false;
        if (incrementSparseIter())
            return true;
        return incrementDenseIter();
    }

    bool SuRF::Iter::decrementDenseIter() {
        if (!dense_iter_.isValid())
            return false;

        dense_iter_--;
        if (!dense_iter_.isValid())
            return false;
        if (dense_iter_.isMoveRightComplete())
            return true;

        passToSparse();
        sparse_iter_.moveToRightMostKey();
        return true;
    }

    bool SuRF::Iter::decrementSparseIter() {
        if (!sparse_iter_.isValid())
            return false;
        sparse_iter_--;
        return sparse_iter_.isValid();
    }

    bool SuRF::Iter::operator--(int) {
        if (!isValid())
            return false;
        if (decrementSparseIter())
            return true;
        return decrementDenseIter();
    }

    bool SuRF::Iter::operator!=(const SuRF::Iter &other) {
        //compare two iterators

        // both iterators invalid
        if (!this->isValid() && !other.isValid()) { return false; }

        // one of them is invalid -> iterators are not equal
        if (!this->isValid() || !other.isValid()) { return true; }

        if (this->dense_iter_.getLastIteratorPosition() != other.dense_iter_.getLastIteratorPosition()) {
            return true;
        }

        // dense iterators are equal and both of them are complete -> search not continued in sparse levels
        if (this->dense_iter_.isComplete() && other.dense_iter_.isComplete()) {
            return false;
        }

        // dense is equal, check sparse levels now
        if (this->sparse_iter_.getLastIteratorPosition() != other.sparse_iter_.getLastIteratorPosition()) {
            return true;
        }
        return false;

    }

} // namespace surf

#endif // SURF_H
