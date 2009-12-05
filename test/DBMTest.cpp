#include <gtest/gtest.h>
#include "bb/DBM.hpp"

namespace bb {

class TestableDBM : public DBM<uint32_t>
{
public:
  using DBM<uint32_t>::fp;
  using DBM<uint32_t>::bucket;
  using DBM<uint32_t>::bucketLength;
  using DBM<uint32_t>::freePool;
  using DBM<uint32_t>::freePoolLength;

  using DBM<uint32_t>::loadMetaData;
  using DBM<uint32_t>::saveMetaData;
  using DBM<uint32_t>::calcBucketIndex;
  using DBM<uint32_t>::calcValueCapacity;
  using DBM<uint32_t>::calcRecordSize;
  using DBM<uint32_t>::findRecordOffset;
  using DBM<uint32_t>::allocNewRecord;
  using DBM<uint32_t>::getFreeArea;
  using DBM<uint32_t>::putFreeArea;
};

}

class DBMTest : public ::testing::Test
{
public:
  static const char* emptyDBMPath;
  static const uint32_t bucketLength;
  static const uint32_t freePoolLength;

protected:
  virtual void SetUp() {
    FILE* fp = fopen(DBMTest::emptyDBMPath, "wb+");

    fwrite(&(DBMTest::bucketLength), sizeof(uint32_t), 1, fp);

    uint32_t* tempBucket = new uint32_t[DBMTest::bucketLength];
    std::fill(tempBucket, tempBucket + DBMTest::bucketLength, 0);
    fwrite(tempBucket, sizeof(uint32_t), DBMTest::bucketLength, fp);
    delete[] tempBucket;

    fwrite(&(DBMTest::freePoolLength), sizeof(uint32_t), 1, fp);

    uint32_t* tempFreePool = new uint32_t[DBMTest::freePoolLength * 2];
    std::fill(tempFreePool, tempFreePool + DBMTest::freePoolLength * 2, 0);
    fwrite(tempFreePool, sizeof(uint32_t), DBMTest::freePoolLength * 2, fp);
    delete[] tempFreePool;
    
    fclose(fp);
  }

  virtual void TearDown() {
    remove(emptyDBMPath);
  }
};

const char* DBMTest::emptyDBMPath = "test.dat";
const uint32_t DBMTest::bucketLength = 1000;
const uint32_t DBMTest::freePoolLength = 1000;

TEST_F(DBMTest, LoadMetaDataTest) {
  bb::TestableDBM* dbm = new bb::TestableDBM();

  dbm->fp = fopen(DBMTest::emptyDBMPath, "rb+");
  dbm->loadMetaData();
  
  EXPECT_EQ(DBMTest::bucketLength, dbm->bucketLength);
  EXPECT_EQ(0, *(dbm->bucket + dbm->bucketLength - 1));
  EXPECT_EQ(DBMTest::freePoolLength, dbm->freePoolLength);
  EXPECT_EQ(0, dbm->freePool->size());

  fclose(dbm->fp);
  dbm->fp = NULL;
  delete dbm;
}

TEST_F(DBMTest, SaveMetaDataTest) {
  bb::TestableDBM* dbm = new bb::TestableDBM();

  dbm->bucketLength = DBMTest::bucketLength * 2;
  dbm->bucket = new uint32_t[dbm->bucketLength];
  std::fill(dbm->bucket, dbm->bucket + this->bucketLength, 0);
  dbm->freePoolLength = 2000;

  dbm->fp = fopen(DBMTest::emptyDBMPath, "rb+");
  dbm->saveMetaData();

  fclose(dbm->fp);
  dbm->fp = NULL;
  delete dbm;

  FILE* fp = fopen(DBMTest::emptyDBMPath, "rb+");

  uint32_t bucketLength;
  EXPECT_EQ(fread(&bucketLength, sizeof(uint32_t), 1, fp), 1);
  ASSERT_EQ(bucketLength, DBMTest::bucketLength * 2);
  uint32_t bucket[bucketLength];
  EXPECT_EQ(fread(bucket, sizeof(uint32_t), bucketLength, fp), bucketLength);

  uint32_t freePoolLength;
  EXPECT_EQ(fread(&freePoolLength, sizeof(uint32_t), 1, fp), 1);
  ASSERT_EQ(freePoolLength, DBMTest::freePoolLength * 2);
  uint32_t freePool[freePoolLength * 2];
  EXPECT_EQ(fread(freePool, sizeof(uint32_t), freePoolLength * 2, fp), freePoolLength * 2);
  
  fclose(fp);
}

TEST_F(DBMTest, CalcBucketIndexTest) {
  bb::TestableDBM* dbm = new bb::TestableDBM();
  dbm->bucketLength = 10;

  EXPECT_LT(dbm->calcBucketIndex("hoge"), dbm->bucketLength);
  EXPECT_LT(dbm->calcBucketIndex("fugafuga"), dbm->bucketLength);
  EXPECT_LT(dbm->calcBucketIndex("longlonglonglonglongkey"), dbm->bucketLength);
  
  delete dbm;
}

TEST_F(DBMTest, CalcValueCapacityTest) {
  uint32_t capacity = bb::TestableDBM::calcValueCapacity(3000);
  EXPECT_EQ(0, capacity % 1024);
  EXPECT_LT(3000, capacity);
}

TEST_F(DBMTest, CalcRecordSizeTest) {
  EXPECT_EQ(4020, bb::TestableDBM::calcRecordSize("hoge", 1000));
  EXPECT_EQ(4024, bb::TestableDBM::calcRecordSize("fugafuga", 1000));
}

TEST_F(DBMTest, GetFreeAreaTest) {
  bb::TestableDBM* dbm = new bb::TestableDBM();
  dbm->freePool->push_back(std::pair<uint32_t, uint32_t>(100, 1000));
  dbm->freePool->push_back(std::pair<uint32_t, uint32_t>(50, 2000));

  EXPECT_EQ(0, dbm->getFreeArea(2001));
  EXPECT_EQ(50, dbm->getFreeArea(2000));
  EXPECT_EQ(0, dbm->getFreeArea(2000));
  EXPECT_EQ(0, dbm->getFreeArea(1001));
  EXPECT_EQ(100, dbm->getFreeArea(1000));
  EXPECT_EQ(0, dbm->getFreeArea(1000));

  delete dbm;
}

TEST_F(DBMTest, PutFreeAreaTest) {
  bb::TestableDBM* dbm = new bb::TestableDBM();
  dbm->freePoolLength = 5;
  dbm->putFreeArea(1, 1000);
  dbm->putFreeArea(2, 2000);
  dbm->putFreeArea(3, 500);
  dbm->putFreeArea(4, 800);
  dbm->putFreeArea(5, 2400);
  dbm->putFreeArea(6, 2200);
  dbm->putFreeArea(7, 200);

  EXPECT_EQ(5, dbm->freePool->size());
  EXPECT_EQ(3, dbm->freePool->at(0).first);
  EXPECT_EQ(500, dbm->freePool->at(0).second);
  EXPECT_EQ(4, dbm->freePool->at(1).first);
  EXPECT_EQ(800, dbm->freePool->at(1).second);
  EXPECT_EQ(1, dbm->freePool->at(2).first);
  EXPECT_EQ(1000, dbm->freePool->at(2).second);
  EXPECT_EQ(2, dbm->freePool->at(3).first);
  EXPECT_EQ(2000, dbm->freePool->at(3).second);
  EXPECT_EQ(5, dbm->freePool->at(4).first);
  EXPECT_EQ(2400, dbm->freePool->at(4).second);
}

TEST_F(DBMTest, AllocNewRecordTest) {
  bb::TestableDBM* dbm = new bb::TestableDBM();

  dbm->fp = fopen(DBMTest::emptyDBMPath, "rb+");
  dbm->bucketLength = DBMTest::bucketLength;
  dbm->bucket = new uint32_t[dbm->bucketLength];
  std::fill(dbm->bucket, dbm->bucket + this->bucketLength, 0);
  dbm->freePoolLength = DBMTest::freePoolLength;

  uint32_t testData[] = {1, 2, 3, 4};
  dbm->allocNewRecord(0, 0, "hoge", testData, 4);

  uint32_t offset;
  for (uint32_t i = 0; i < dbm->bucketLength; ++i) {
    if (*(dbm->bucket + i) != 0) {
      offset = *(dbm->bucket + i);
      break;
    }
  }
  fseek(dbm->fp, offset, SEEK_SET);

  uint32_t nextOffset;
  EXPECT_EQ(fread(&nextOffset, sizeof(uint32_t), 1, dbm->fp), 1);
  EXPECT_EQ(0, nextOffset);

  uint32_t keyLength;
  EXPECT_EQ(fread(&keyLength, sizeof(uint32_t), 1, dbm->fp), 1);
  EXPECT_EQ(4, keyLength);
  char key[keyLength + 1];
  key[keyLength] = '\0';
  ASSERT_EQ(fread(key, sizeof(char), keyLength, dbm->fp), keyLength);
  EXPECT_STREQ("hoge", key);

  uint32_t valueCapacity;
  uint32_t valueLength;
  EXPECT_EQ(fread(&valueCapacity, sizeof(uint32_t), 1, dbm->fp), 1);
  ASSERT_EQ(valueCapacity, 1024);
  EXPECT_EQ(fread(&valueLength, sizeof(uint32_t), 1, dbm->fp), 1);
  EXPECT_EQ(valueLength, 4);
  uint32_t value[valueCapacity];
  ASSERT_EQ(fread(value, sizeof(uint32_t), valueCapacity, dbm->fp), valueCapacity);
  EXPECT_EQ(value[0], 1);
  EXPECT_EQ(value[1], 2);
  EXPECT_EQ(value[2], 3);
  EXPECT_EQ(value[3], 4);
  EXPECT_EQ(value[4], 0);
  EXPECT_EQ(value[valueCapacity - 1], 0);

  uint32_t testData2[] = {5, 7, 9};
  dbm->allocNewRecord(offset, 100, "fugafuga", testData2, 3);
  fseek(dbm->fp, offset, SEEK_SET);

  uint32_t offset2;
  ASSERT_EQ(fread(&offset2, sizeof(uint32_t), 1, dbm->fp), 1);
  ASSERT_NE(0, offset2);

  fseek(dbm->fp, offset2, SEEK_SET);

  uint32_t nextOffset2;
  EXPECT_EQ(fread(&nextOffset2, sizeof(uint32_t), 1, dbm->fp), 1);
  EXPECT_EQ(100, nextOffset2);

  uint32_t keyLength2;
  EXPECT_EQ(fread(&keyLength2, sizeof(uint32_t), 1, dbm->fp), 1);
  EXPECT_EQ(8, keyLength2);
  char key2[keyLength2 + 1];
  key2[keyLength2] = '\0';
  ASSERT_EQ(fread(key2, sizeof(char), keyLength2, dbm->fp), keyLength2);
  EXPECT_STREQ("fugafuga", key2);

  uint32_t valueCapacity2;
  uint32_t valueLength2;
  EXPECT_EQ(fread(&valueCapacity2, sizeof(uint32_t), 1, dbm->fp), 1);
  ASSERT_EQ(valueCapacity2, 1024);
  EXPECT_EQ(fread(&valueLength2, sizeof(uint32_t), 1, dbm->fp), 1);
  EXPECT_EQ(valueLength2, 3);
  uint32_t value2[valueCapacity2];
  ASSERT_EQ(fread(value2, sizeof(uint32_t), valueCapacity2, dbm->fp), valueCapacity2);
  EXPECT_EQ(value2[0], 5);
  EXPECT_EQ(value2[1], 7);
  EXPECT_EQ(value2[2], 9);
  EXPECT_EQ(value2[3], 0);
  EXPECT_EQ(value2[valueCapacity2 - 1], 0);
  
  delete dbm;
}

TEST_F(DBMTest, FindRecordOffsetTest) {
  bb::TestableDBM* dbm = new bb::TestableDBM(); 
  dbm->fp = fopen(DBMTest::emptyDBMPath, "rb+");
  dbm->bucketLength = DBMTest::bucketLength;
  dbm->bucket = new uint32_t[dbm->bucketLength];
  std::fill(dbm->bucket, dbm->bucket + this->bucketLength, 0);
  dbm->freePoolLength = DBMTest::freePoolLength;

  uint32_t prevOffset;
  uint32_t offset;
  uint32_t nextOffset;
  dbm->findRecordOffset("hoge", &prevOffset, &offset, &nextOffset);
  EXPECT_EQ(offset, 0);
  ASSERT_EQ(prevOffset, 0);  
  EXPECT_EQ(nextOffset, 0);  

  uint32_t testData[] = {1, 2, 3, 4};
  dbm->allocNewRecord(0, 0, "fuga", testData, 4);
  uint32_t actualOffset;
  for (uint32_t i = 0; i < dbm->bucketLength; ++i) {
    if (*(dbm->bucket + i) != 0) {
      actualOffset = *(dbm->bucket + i);
      break;
    }
  }
  std::fill(dbm->bucket, dbm->bucket + this->bucketLength, actualOffset);
  dbm->allocNewRecord(actualOffset, 0, "hoge", testData, 4);

  dbm->findRecordOffset("fuga", &prevOffset, &offset, &nextOffset);
  EXPECT_EQ(offset, actualOffset);
  EXPECT_EQ(prevOffset, 0);  
  EXPECT_NE(nextOffset, 0);  

  dbm->findRecordOffset("hoge", &prevOffset, &offset, &nextOffset);
  EXPECT_NE(offset, 0);
  EXPECT_EQ(prevOffset, actualOffset);  
  EXPECT_EQ(nextOffset, 0);  

  delete dbm;
}

TEST_F(DBMTest, OpenTest) {
  bb::TestableDBM* dbm = new bb::TestableDBM(); 

  EXPECT_EQ(false, dbm->open(NULL));
  EXPECT_EQ(false, dbm->open("not_exist.dat"));
  EXPECT_EQ(true, dbm->open(DBMTest::emptyDBMPath));
  EXPECT_EQ(DBMTest::bucketLength, dbm->bucketLength);
  EXPECT_EQ(0, *(dbm->bucket + dbm->bucketLength - 1));
  EXPECT_EQ(DBMTest::freePoolLength, dbm->freePoolLength);
  EXPECT_EQ(0, dbm->freePool->size());

  fclose(dbm->fp);  
  dbm->fp = NULL;  
  delete dbm;
}

TEST_F(DBMTest, CreateTest) {
  bb::TestableDBM* dbm = new bb::TestableDBM(); 

  EXPECT_EQ(false, dbm->create(NULL, 2000, 1000));
  EXPECT_EQ(true, dbm->create("not_exist.dat", 2000, 1000));

  EXPECT_EQ(2000, dbm->bucketLength);
  EXPECT_EQ(0, *(dbm->bucket + dbm->bucketLength - 1));
  EXPECT_EQ(1000, dbm->freePoolLength);  
  EXPECT_EQ(0, dbm->freePool->size());  

  fclose(dbm->fp);  
  dbm->fp = NULL;  
  delete dbm;

  FILE* fp = fopen("not_exist.dat", "rb+");

  uint32_t bucketLength;
  EXPECT_EQ(1, fread(&bucketLength, sizeof(uint32_t), 1, fp));
  ASSERT_EQ(2000, bucketLength);
  uint32_t bucket[bucketLength];
  EXPECT_EQ(bucketLength, fread(bucket, sizeof(uint32_t), bucketLength, fp));

  uint32_t freePoolLength;
  EXPECT_EQ(1, fread(&freePoolLength, sizeof(uint32_t), 1, fp));
  ASSERT_EQ(1000, freePoolLength);
  uint32_t freePool[freePoolLength * 2];
  EXPECT_EQ(2000, fread(freePool, sizeof(uint32_t), freePoolLength * 2, fp));
  EXPECT_EQ(0, *(freePool + freePoolLength * 2 - 1));
  
  fclose(fp);

  remove("not_exist.dat");
}

TEST_F(DBMTest, GetTest) {
  bb::TestableDBM* dbm = new bb::TestableDBM(); 
  dbm->fp = fopen(DBMTest::emptyDBMPath, "rb+");
  dbm->bucketLength = DBMTest::bucketLength;
  dbm->bucket = new uint32_t[dbm->bucketLength];
  std::fill(dbm->bucket, dbm->bucket + this->bucketLength, 0);
  dbm->freePoolLength = DBMTest::freePoolLength;

  uint32_t testData[] = {1, 2, 3, 4};
  dbm->allocNewRecord(0, 0, "fuga", testData, 4);
  
  uint32_t valueLength;
  EXPECT_EQ(NULL, dbm->get("hoge", &valueLength));
  EXPECT_EQ(0, valueLength);
  
  uint32_t* value = dbm->get("fuga", &valueLength);
  ASSERT_EQ(4, valueLength);
  EXPECT_EQ(1, *value);
  EXPECT_EQ(2, *(value + 1));
  EXPECT_EQ(3, *(value + 2));
  EXPECT_EQ(4, *(value + 3));

  fclose(dbm->fp);  
  dbm->fp = NULL;  
  delete dbm;
}

TEST_F(DBMTest, SetTest) {
  bb::TestableDBM* dbm = new bb::TestableDBM(); 
  dbm->fp = fopen(DBMTest::emptyDBMPath, "rb+");
  dbm->bucketLength = DBMTest::bucketLength;
  dbm->bucket = new uint32_t[dbm->bucketLength];
  std::fill(dbm->bucket, dbm->bucket + this->bucketLength, 0);
  dbm->freePoolLength = DBMTest::freePoolLength;

  uint32_t testData[] = {1, 2, 3};
  dbm->set("fuga", testData, 3);

  uint32_t valueLength;
  uint32_t* value = dbm->get("fuga", &valueLength);
  ASSERT_EQ(3, valueLength);
  EXPECT_EQ(1, *value);
  EXPECT_EQ(2, *(value + 1));
  EXPECT_EQ(3, *(value + 2));
  delete[] value;

  uint32_t testData2[800];
  std::fill(testData2, testData2 + 800, 100);
  dbm->set("fuga", testData2, 800);

  value = dbm->get("fuga", &valueLength);
  ASSERT_EQ(800, valueLength);
  EXPECT_EQ(100, *value);
  EXPECT_EQ(100, *(value + 799));
  delete[] value;

  uint32_t testData3[2000];
  std::fill(testData3, testData3 + 2000, 200);
  dbm->set("fuga", testData3, 2000);    

  value = dbm->get("fuga", &valueLength);
  ASSERT_EQ(2000, valueLength);
  EXPECT_EQ(200, *value);
  EXPECT_EQ(200, *(value + 1999));
  delete[] value;
  
  fclose(dbm->fp);  
  dbm->fp = NULL;  
  delete dbm;
}

TEST_F(DBMTest, RemoveTest) {
  bb::TestableDBM* dbm = new bb::TestableDBM(); 
  dbm->fp = fopen(DBMTest::emptyDBMPath, "rb+");
  dbm->bucketLength = DBMTest::bucketLength;
  dbm->bucket = new uint32_t[dbm->bucketLength];
  std::fill(dbm->bucket, dbm->bucket + this->bucketLength, 0);
  dbm->freePoolLength = DBMTest::freePoolLength;

  uint32_t testData[] = {1, 2, 3, 4};
  dbm->allocNewRecord(0, 0, "fuga", testData, 4);
  uint32_t actualOffset;
  uint32_t index;
  for (index = 0; index < dbm->bucketLength; ++index) {
    if (*(dbm->bucket + index) != 0) {
      actualOffset = *(dbm->bucket + index);
      break;
    }
  }
  std::fill(dbm->bucket, dbm->bucket + this->bucketLength, actualOffset);
  dbm->allocNewRecord(actualOffset, 0, "hoge", testData, 4);

  uint32_t prevOffset;
  uint32_t offset;
  uint32_t nextOffset;
  dbm->findRecordOffset("hoge", &prevOffset, &offset ,&nextOffset);
  EXPECT_NE(0, offset);
  EXPECT_EQ(actualOffset, prevOffset);
  EXPECT_EQ(0, nextOffset);
  EXPECT_EQ(0, dbm->freePool->size());

  dbm->remove("hoge");

  dbm->findRecordOffset("hoge", &prevOffset, &offset ,&nextOffset);
  EXPECT_EQ(0, offset);

  dbm->findRecordOffset("fuga", &prevOffset, &offset ,&nextOffset);
  EXPECT_EQ(actualOffset, offset);
  EXPECT_EQ(0, prevOffset);
  EXPECT_EQ(0, nextOffset);
  EXPECT_EQ(1, dbm->freePool->size());

  dbm->remove("fuga");

  dbm->findRecordOffset("fuga", &prevOffset, &offset ,&nextOffset);
  EXPECT_EQ(0, offset);
  EXPECT_EQ(0, prevOffset);
  EXPECT_EQ(0, *(dbm->bucket + index));
  ASSERT_EQ(2, dbm->freePool->size());
  EXPECT_EQ(actualOffset, dbm->freePool->at(1).first);
  EXPECT_EQ(dbm->calcRecordSize("fuga", 1024), dbm->freePool->at(1).second);

  fclose(dbm->fp);  
  dbm->fp = NULL;  
  delete dbm;  
}

TEST_F(DBMTest, AppendTest) {
  bb::TestableDBM* dbm = new bb::TestableDBM(); 
  dbm->fp = fopen(DBMTest::emptyDBMPath, "rb+");
  dbm->bucketLength = DBMTest::bucketLength;
  dbm->bucket = new uint32_t[dbm->bucketLength];
  std::fill(dbm->bucket, dbm->bucket + this->bucketLength, 0);
  dbm->freePoolLength = DBMTest::freePoolLength;
  
  uint32_t originalValue[400];
  std::fill(originalValue, originalValue + 400, 0);
  dbm->append("fuga", originalValue, 400);

  uint32_t* value;
  uint32_t valueLength;
  value = dbm->get("fuga", &valueLength);
  ASSERT_EQ(400, valueLength);
  EXPECT_EQ(0, *value);
  EXPECT_EQ(0, *(value + 399));
  EXPECT_EQ(0, dbm->freePool->size());
  delete[] value;

  std::fill(originalValue, originalValue + 400, 100);
  dbm->append("fuga", originalValue, 400);

  value = dbm->get("fuga", &valueLength);
  ASSERT_EQ(800, valueLength);
  EXPECT_EQ(0, *value);
  EXPECT_EQ(0, *(value + 399));
  EXPECT_EQ(100, *(value + 400));
  EXPECT_EQ(100, *(value + 799));
  EXPECT_EQ(0, dbm->freePool->size());
  delete[] value;

  uint32_t prevOffset;
  uint32_t offset;
  uint32_t nextOffset;
  dbm->findRecordOffset("fuga", &prevOffset, &offset, &nextOffset);

  std::fill(originalValue, originalValue + 400, 200);
  dbm->append("fuga", originalValue, 400);

  value = dbm->get("fuga", &valueLength);
  ASSERT_EQ(1200, valueLength);
  EXPECT_EQ(0, *value);
  EXPECT_EQ(0, *(value + 399));
  EXPECT_EQ(100, *(value + 400));
  EXPECT_EQ(100, *(value + 799));
  EXPECT_EQ(200, *(value + 800));
  EXPECT_EQ(200, *(value + 1199));
  ASSERT_EQ(1, dbm->freePool->size());
  EXPECT_EQ(offset, dbm->freePool->at(0).first);
  EXPECT_EQ(dbm->calcRecordSize("fuga", 1024), dbm->freePool->at(0).second);
  delete[] value;

  fclose(dbm->fp);  
  dbm->fp = NULL;  
  delete dbm;    
}

TEST_F(DBMTest, ContainsTest) {
  bb::TestableDBM* dbm = new bb::TestableDBM(); 
  dbm->fp = fopen(DBMTest::emptyDBMPath, "rb+");
  dbm->bucketLength = DBMTest::bucketLength;
  dbm->bucket = new uint32_t[dbm->bucketLength];
  std::fill(dbm->bucket, dbm->bucket + this->bucketLength, 0);
  dbm->freePoolLength = DBMTest::freePoolLength;

  EXPECT_EQ(false, dbm->contains("hoge"));

  uint32_t testData[] = {1, 2, 3};
  dbm->set("hoge", testData, 3);  

  EXPECT_EQ(true, dbm->contains("hoge"));

  fclose(dbm->fp);  
  dbm->fp = NULL;  
  delete dbm;    
}
