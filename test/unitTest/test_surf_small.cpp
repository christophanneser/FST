#include "gtest/gtest.h"

#include <assert.h>

#include <string>
#include <vector>

#include "config.hpp"
#include "surf.hpp"

namespace surf {

namespace surftest {

static const SuffixType kSuffixType = kReal;
static const level_t kSuffixLen = 8;

class SuRFInt32Test : public ::testing::Test {
public:
    virtual void SetUp () {}
    virtual void TearDown () {}
};

TEST_F (SuRFInt32Test, ExampleInPaperTest) {
    std::vector<std::string> keys;

    keys.emplace_back(std::string("aaaa"));
    keys.emplace_back(std::string("aaab"));
    keys.emplace_back(std::string("aaac"));
    keys.emplace_back(std::string("abaa"));
    keys.emplace_back(std::string("abab"));
    keys.emplace_back(std::string("abac"));
    keys.emplace_back(std::string("ac"));
    keys.emplace_back(std::string("baaa"));
    keys.emplace_back(std::string("baab"));
    keys.emplace_back(std::string("baac"));
    keys.emplace_back(std::string("bbaa"));
    keys.emplace_back(std::string("bbab"));
    keys.emplace_back(std::string("bbac"));
    keys.emplace_back(std::string("cabc"));
    keys.emplace_back(std::string("cabd"));
    keys.emplace_back(std::string("cacc"));
    keys.emplace_back(std::string("cacd"));
    keys.emplace_back(std::string("d"));
    keys.emplace_back(std::string("e"));

    std::vector<uint64_t > values;
    for (uint64_t i = 0; i < keys.size(); i++) values.emplace_back(i);

    SuRF* surf = new SuRF(keys, values, kIncludeDense, kSparseDenseRatio, kSuffixType, 0, kSuffixLen);

    for (uint64_t i = 0; i < values.size(); i++) {
        uint64_t value = 0;
        bool exist = surf->lookupKey(keys[i], value);
        ASSERT_TRUE(exist);
        ASSERT_EQ(i, value);
    }

    SuRF::Iter iter = surf->moveToKeyGreaterThan(std::string("t"), true);
    ASSERT_TRUE(iter.isValid());
    iter++;
    ASSERT_TRUE(iter.isValid());
}

} // namespace surftest

} // namespace surf

int main (int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
