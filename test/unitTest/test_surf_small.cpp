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

class SuRFSmallTest : public ::testing::Test {
public:
    virtual void SetUp () {}
    virtual void TearDown () {}
};

TEST_F (SuRFSmallTest, ExampleInPaperTest) {
    std::vector<std::string> keys;

    keys.emplace_back(std::string("aaaa"));
    keys.emplace_back(std::string("aaab"));
    keys.emplace_back(std::string("aaac"));
    keys.emplace_back(std::string("abaa"));
    keys.emplace_back(std::string("abab"));
    keys.emplace_back(std::string("abac"));
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

    SuRF* surf = new SuRF(keys, kIncludeDense, kSparseDenseRatio, kSuffixType, 0, kSuffixLen);
    //bool exist = surf->lookupRange(std::string("top"), false, std::string("toyy"), false);
    bool exist = surf->lookupKey(std::string("topless"));
    ASSERT_TRUE(exist);
    exist = surf->lookupRange(std::string("toq"), false, std::string("toyy"), false);
    ASSERT_TRUE(exist);
    exist = surf->lookupRange(std::string("trie"), false, std::string("tripp"), false);
    ASSERT_TRUE(exist);

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
