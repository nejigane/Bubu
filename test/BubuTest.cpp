#include <gtest/gtest.h>
#include "bb/Bubu.hpp"

namespace bb {

class TestableBubu : public Bubu
{
public:
  using Bubu::index;
  using Bubu::library;

  using Bubu::uintToString;
  using Bubu::tokenizeUTF8;
};

}

class BubuTest : public ::testing::Test
{
protected:
  virtual void SetUp() {
    remove("bubu.idx");
    remove("bubu.lib");
  }
  
  virtual void TearDown() {
    remove("bubu.idx");
    remove("bubu.lib");
  }
};

TEST_F(BubuTest, UintToStringTest) {
  EXPECT_EQ(0, bb::TestableBubu::uintToString(123).compare("123"));
}

TEST_F(BubuTest, TokenizeUTF8Test) {
  std::vector<std::string> unigrams;
  std::vector<std::string> bigrams;
  bb::TestableBubu::tokenizeUTF8("hogefuga", true, unigrams, bigrams);
  
  ASSERT_EQ(8, unigrams.size());
  EXPECT_EQ(0, unigrams.at(0).compare("h"));
  EXPECT_EQ(0, unigrams.at(1).compare("o"));
  EXPECT_EQ(0, unigrams.at(2).compare("g"));
  EXPECT_EQ(0, unigrams.at(3).compare("e"));
  EXPECT_EQ(0, unigrams.at(4).compare("f"));
  EXPECT_EQ(0, unigrams.at(5).compare("u"));
  EXPECT_EQ(0, unigrams.at(6).compare("g"));
  EXPECT_EQ(0, unigrams.at(7).compare("a"));

  ASSERT_EQ(7, bigrams.size());
  EXPECT_EQ(0, bigrams.at(0).compare("ho"));
  EXPECT_EQ(0, bigrams.at(1).compare("og"));
  EXPECT_EQ(0, bigrams.at(2).compare("ge"));
  EXPECT_EQ(0, bigrams.at(3).compare("ef"));
  EXPECT_EQ(0, bigrams.at(4).compare("fu"));
  EXPECT_EQ(0, bigrams.at(5).compare("ug"));
  EXPECT_EQ(0, bigrams.at(6).compare("ga"));

  bb::TestableBubu::tokenizeUTF8("hogefug", false, unigrams, bigrams);

  ASSERT_EQ(7, unigrams.size());
  EXPECT_EQ(0, unigrams.at(0).compare("h"));
  EXPECT_EQ(0, unigrams.at(1).compare("o"));
  EXPECT_EQ(0, unigrams.at(2).compare("g"));
  EXPECT_EQ(0, unigrams.at(3).compare("e"));
  EXPECT_EQ(0, unigrams.at(4).compare("f"));
  EXPECT_EQ(0, unigrams.at(5).compare("u"));
  EXPECT_EQ(0, unigrams.at(6).compare("g"));

  ASSERT_EQ(3, bigrams.size());
  EXPECT_EQ(0, bigrams.at(0).compare("ho"));
  EXPECT_EQ(0, bigrams.at(1).compare("ge"));
  EXPECT_EQ(0, bigrams.at(2).compare("fu"));

  bb::TestableBubu::tokenizeUTF8("ほげふがひ", false, unigrams, bigrams);

  ASSERT_EQ(5, unigrams.size());
  EXPECT_EQ(0, unigrams.at(0).compare("ほ"));
  EXPECT_EQ(0, unigrams.at(1).compare("げ"));
  EXPECT_EQ(0, unigrams.at(2).compare("ふ"));
  EXPECT_EQ(0, unigrams.at(3).compare("が"));
  EXPECT_EQ(0, unigrams.at(4).compare("ひ"));

  ASSERT_EQ(2, bigrams.size());
  EXPECT_EQ(0, bigrams.at(0).compare("ほげ"));
  EXPECT_EQ(0, bigrams.at(1).compare("ふが"));
}

TEST_F(BubuTest, OpenTest) {
  bb::TestableBubu* bubu = new bb::TestableBubu();

  EXPECT_FALSE(bubu->open("."));

  bb::DBM<uint32_t>* index = new bb::DBM<uint32_t>();
  index->create("bubu.idx", 100, 100);
  index->close();
  delete index;

  bb::DBM<char>* library = new bb::DBM<char>();
  library->create("bubu.lib", 100, 100);
  library->close();
  delete library;

  EXPECT_TRUE(bubu->open("."));
  bubu->close();

  delete bubu;
}

TEST_F(BubuTest, CreateTest) {
  bb::TestableBubu* bubu = new bb::TestableBubu();

  EXPECT_TRUE(bubu->create("."));
  
  bubu->close();
  delete bubu;

  FILE* fp;
  fp = fopen("bubu.idx", "r");
  ASSERT_TRUE(fp != NULL);
  fclose(fp);

  fp = fopen("bubu.lib", "r");
  ASSERT_TRUE(fp != NULL);
  fclose(fp);
}

TEST_F(BubuTest, RegisterDocTest) {
  bb::TestableBubu* bubu = new bb::TestableBubu();  
  bubu->create(".");

  bubu->registerDoc(1, "テスト");

  uint32_t docLength;
  char* doc = bubu->library->get("1", &docLength);
  ASSERT_TRUE(docLength > 0);
  EXPECT_EQ(0, strncmp("テスト", doc, docLength));
  delete[] doc;

  uint32_t valueLength;
  uint32_t* value = bubu->index->get("テ", &valueLength);
  ASSERT_EQ(2, valueLength);
  EXPECT_EQ(1, *value);
  EXPECT_EQ(0, *(value + 1));
  delete[] value;
  
  value = bubu->index->get("ス", &valueLength);
  ASSERT_EQ(2, valueLength);
  EXPECT_EQ(1, *value);
  EXPECT_EQ(1, *(value + 1));
  delete[] value;

  value = bubu->index->get("ト", &valueLength);
  ASSERT_EQ(2, valueLength);
  EXPECT_EQ(1, *value);
  EXPECT_EQ(2, *(value + 1));
  delete[] value;

  value = bubu->index->get("テス", &valueLength);
  ASSERT_EQ(2, valueLength);
  EXPECT_EQ(1, *value);
  EXPECT_EQ(0, *(value + 1));
  delete[] value;

  value = bubu->index->get("スト", &valueLength);
  ASSERT_EQ(2, valueLength);
  EXPECT_EQ(1, *value);
  EXPECT_EQ(1, *(value + 1));
  delete[] value;

  bubu->registerDoc(2, "ストア");

  value = bubu->index->get("ス", &valueLength);
  ASSERT_EQ(4, valueLength);
  EXPECT_EQ(1, *value);
  EXPECT_EQ(1, *(value + 1));
  EXPECT_EQ(2, *(value + 2));
  EXPECT_EQ(0, *(value + 3));
  delete[] value;

  value = bubu->index->get("ト", &valueLength);
  ASSERT_EQ(4, valueLength);
  EXPECT_EQ(1, *value);
  EXPECT_EQ(2, *(value + 1));
  EXPECT_EQ(2, *(value + 2));
  EXPECT_EQ(1, *(value + 3));
  delete[] value;

  value = bubu->index->get("ア", &valueLength);
  ASSERT_EQ(2, valueLength);
  EXPECT_EQ(2, *value);
  EXPECT_EQ(2, *(value + 1));
  delete[] value;

  value = bubu->index->get("スト", &valueLength);
  ASSERT_EQ(4, valueLength);
  EXPECT_EQ(1, *value);
  EXPECT_EQ(1, *(value + 1));
  EXPECT_EQ(2, *(value + 2));
  EXPECT_EQ(0, *(value + 3));
  delete[] value;

  value = bubu->index->get("トア", &valueLength);
  ASSERT_EQ(2, valueLength);
  EXPECT_EQ(2, *value);
  EXPECT_EQ(1, *(value + 1));
  delete[] value;
  
  delete bubu;
}

TEST_F(BubuTest, UnregisterDocTest) {
  bb::TestableBubu* bubu = new bb::TestableBubu();  
  bubu->create(".");

  bubu->registerDoc(1, "テスト");
  bubu->registerDoc(2, "ストア");
  bubu->unregisterDoc(1);

  uint32_t docLength;
  char* doc = bubu->library->get("1", &docLength);
  EXPECT_TRUE(docLength == 0);
  EXPECT_TRUE(doc == NULL);

  doc = bubu->library->get("2", &docLength);
  ASSERT_TRUE(docLength > 0);
  EXPECT_EQ(0, strncmp("ストア", doc, docLength));
  delete[] doc;
  
  uint32_t valueLength;
  uint32_t* value = bubu->index->get("テ", &valueLength);
  ASSERT_EQ(0, valueLength);
  EXPECT_TRUE(value == NULL);
  
  value = bubu->index->get("ス", &valueLength);
  ASSERT_EQ(2, valueLength);
  EXPECT_EQ(2, *value);
  EXPECT_EQ(0, *(value + 1));
  delete[] value;

  value = bubu->index->get("ト", &valueLength);
  ASSERT_EQ(2, valueLength);
  EXPECT_EQ(2, *value);
  EXPECT_EQ(1, *(value + 1));
  delete[] value;

  value = bubu->index->get("ア", &valueLength);
  ASSERT_EQ(2, valueLength);
  EXPECT_EQ(2, *value);
  EXPECT_EQ(2, *(value + 1));
  delete[] value;
  
  value = NULL;
  value = bubu->index->get("テス", &valueLength);
  ASSERT_EQ(0, valueLength);
  EXPECT_TRUE(value == NULL);

  value = bubu->index->get("スト", &valueLength);
  ASSERT_EQ(2, valueLength);
  EXPECT_EQ(2, *value);
  EXPECT_EQ(0, *(value + 1));
  delete[] value;

  value = bubu->index->get("トア", &valueLength);
  ASSERT_EQ(2, valueLength);
  EXPECT_EQ(2, *value);
  EXPECT_EQ(1, *(value + 1));
  delete[] value;

  delete bubu;
}

TEST_F(BubuTest, GetDocContentTest) {
  bb::TestableBubu* bubu = new bb::TestableBubu();  
  bubu->create(".");

  bubu->registerDoc(1, "テスト");
  bubu->registerDoc(2, "ほげほげ");
  EXPECT_STREQ("テスト", bubu->getDocContent(1).c_str());
  EXPECT_STREQ("ほげほげ", bubu->getDocContent(2).c_str());
  EXPECT_STREQ("", bubu->getDocContent(3).c_str());

  delete bubu;
}

TEST_F(BubuTest, SearchTest) {
  bb::TestableBubu* bubu = new bb::TestableBubu();  
  bubu->create(".");

  bubu->registerDoc(1, "本日は、快晴なり。");
  bubu->registerDoc(2, "明後日は、仕事。今度の休日は、お出かけ");
  bubu->registerDoc(3, "東京タワーは、結構高い");

  std::vector<std::pair<uint32_t, uint32_t> > hits = bubu->search("日は、");

  ASSERT_EQ(3, hits.size());
  EXPECT_EQ(1, hits.at(0).first);
  EXPECT_EQ(1, hits.at(0).second);
  EXPECT_EQ(2, hits.at(1).first);
  EXPECT_EQ(2, hits.at(1).second);
  EXPECT_EQ(2, hits.at(2).first);
  EXPECT_EQ(12, hits.at(2).second);

  std::vector<std::pair<uint32_t, uint32_t> > hits2 = bubu->search("検索エンジン");
  EXPECT_EQ(0, hits2.size());

  delete bubu;
}
