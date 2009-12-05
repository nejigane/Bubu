/**
 * Bubu.hpp
 *
 * @author      Yu Nejigane
 * @link        http://wiki.github.com/nejigane/Bubu
 *
 * Copyright (c) 2009 Yu Nejigane
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#ifndef BB_DBM_HPP_
#define BB_DBM_HPP_

#include <stdint.h>
#include <vector>

namespace bb {

template<typename V>
class DBM 
{
protected:
  static const uint32_t NULL_OFFSET;
  static const uint32_t INITIAL_CAPACITY;

  static uint32_t calcValueCapacity(uint32_t valueLength);
  static uint32_t calcRecordSize(const char* key, uint32_t valueCapacity);

  FILE* fp;
  uint32_t* bucket;
  uint32_t bucketLength;
  std::vector<std::pair<uint32_t, uint32_t> >* freePool;
  uint32_t freePoolLength;

  void loadMetaData();
  void saveMetaData();
  uint32_t calcBucketIndex(const char* key);
  void findRecordOffset(const char* key, uint32_t* prevOffset, uint32_t* offset, uint32_t* nextOffset);
  void allocNewRecord(uint32_t prevOffset, uint32_t nextOffset, const char* key, const V* value, uint32_t valueLength);
  uint32_t getFreeArea(uint32_t requisiteSize);
  void putFreeArea(uint32_t offset, uint32_t size);

public:
  DBM();
  virtual ~DBM();
  bool open(const char* path);
  bool create(const char* path, uint32_t bucketLength, uint32_t freePoolLength);
  void close();
  V* get(const char* key, uint32_t* valueLength);
  void set(const char* key, const V* value, uint32_t valueLength);
  void remove(const char* key);
  void append(const char* key, const V* value, uint32_t valueLength);
  bool contains(const char* key);
};

template <typename V> const uint32_t DBM<V>::NULL_OFFSET = 0;
template <typename V> const uint32_t DBM<V>::INITIAL_CAPACITY = 1024;

template <typename V>
DBM<V>::DBM() : fp(NULL), bucket(NULL), bucketLength(0), freePoolLength(0)
{
  this->freePool = new std::vector<std::pair<uint32_t, uint32_t> >;
}

template <typename V>
DBM<V>::~DBM()
{
  this->close();
  if (this->bucket) delete[] this->bucket;
  delete this->freePool;
}

template <typename V>
bool DBM<V>::open(const char* path)
{
  if (path == NULL || (this->fp = fopen(path, "rb+")) == NULL) return false;

  this->loadMetaData();

  return true;
}

template <typename V>
bool DBM<V>::create(const char* path, uint32_t bucketLength, uint32_t freePoolLength)
{
  if (path == NULL || (this->fp = fopen(path, "wb+")) == NULL) return false;

  this->bucket = new uint32_t[bucketLength];
  std::fill(this->bucket, this->bucket + bucketLength, DBM::NULL_OFFSET);
  this->bucketLength = bucketLength;

  this->freePool->clear();
  this->freePoolLength = freePoolLength;

  this->saveMetaData();

  return true;
}

template <typename V>
void DBM<V>::close()
{
  if (this->fp) {
    this->saveMetaData();
    fclose(this->fp);
    this->fp = NULL;
  }
}

template <typename V>
V* DBM<V>::get(const char* key, uint32_t* valueLength)
{
  uint32_t prevOffset;
  uint32_t offset;
  uint32_t nextOffset;
  this->findRecordOffset(key, &prevOffset, &offset, &nextOffset);
  
  if (offset == DBM::NULL_OFFSET) {
    *valueLength = 0;
    return NULL;
  }

  fseek(this->fp, sizeof(uint32_t), SEEK_CUR);
  fread(valueLength, sizeof(uint32_t), 1, this->fp);

  V* value = new V[*valueLength];
  fread(value, sizeof(V), *valueLength, this->fp);
  
  return value;
}

template <typename V>
void DBM<V>::set(const char* key, const V* value, uint32_t valueLength)
{
  uint32_t prevOffset;
  uint32_t offset;
  uint32_t nextOffset;
  this->findRecordOffset(key, &prevOffset, &offset, &nextOffset);
  
  if (offset == DBM::NULL_OFFSET) {
    this->allocNewRecord(prevOffset, DBM::NULL_OFFSET, key, value, valueLength);
    return;
  }

  uint32_t oldValueCapacity;
  fread(&oldValueCapacity, sizeof(uint32_t), 1, this->fp);

  if (valueLength <= oldValueCapacity) {
    fseek(this->fp, 0, SEEK_CUR);
    fwrite(&valueLength, sizeof(uint32_t), 1, this->fp);
    fwrite(value, sizeof(V), valueLength, this->fp);
  }
  else {
    this->putFreeArea(offset, DBM<V>::calcRecordSize(key, oldValueCapacity));
    this->allocNewRecord(prevOffset, nextOffset, key, value, valueLength);
  }
}

template <typename V>
void DBM<V>::append(const char* key, const V* value, uint32_t valueLength)
{
  uint32_t prevOffset;
  uint32_t offset;
  uint32_t nextOffset;
  this->findRecordOffset(key, &prevOffset, &offset, &nextOffset);

  if (offset == DBM::NULL_OFFSET) {
    this->allocNewRecord(prevOffset, DBM::NULL_OFFSET, key, value, valueLength);
    return;
  }

  uint32_t oldValueCapacity;
  uint32_t oldValueLength;
  fread(&oldValueCapacity, sizeof(uint32_t), 1, this->fp);
  fread(&oldValueLength, sizeof(uint32_t), 1, this->fp);

  uint32_t newValueLength = oldValueLength + valueLength;
  if (newValueLength <= oldValueCapacity) {
    fseek(this->fp, -1 * sizeof(uint32_t), SEEK_CUR);
    fwrite(&newValueLength, sizeof(uint32_t), 1, this->fp);
    fseek(this->fp, sizeof(V) * oldValueLength, SEEK_CUR);
    fwrite(value, sizeof(V), valueLength, this->fp);
  }
  else {
    V* newValue = new V[newValueLength];
    fread(newValue, sizeof(V), oldValueLength, this->fp);
    memcpy(newValue + oldValueLength, value, sizeof(V) * valueLength);

    this->putFreeArea(offset, DBM<V>::calcRecordSize(key, oldValueCapacity));
    this->allocNewRecord(prevOffset, nextOffset, key, newValue, newValueLength);
  }
}

template <typename V>
void DBM<V>::allocNewRecord(uint32_t prevOffset, uint32_t nextOffset, const char* key, const V* value, uint32_t valueLength)
{
  uint32_t valueCapacity = DBM<V>::calcValueCapacity(valueLength);
  uint32_t keyLength = strlen(key);
  uint32_t requisiteSize = DBM<V>::calcRecordSize(key, valueCapacity);

  uint32_t newOffset = this->getFreeArea(requisiteSize);
  if (newOffset == DBM::NULL_OFFSET) {
    fseek(this->fp, 0, SEEK_END);
    newOffset = (uint32_t) ftell(this->fp);
  }
  else {
    fseek(this->fp, newOffset, SEEK_SET);
  }

  fwrite(&nextOffset, sizeof(uint32_t), 1, this->fp);
  fwrite(&keyLength, sizeof(uint32_t), 1, this->fp);
  fwrite(key, sizeof(char), keyLength, this->fp);
  fwrite(&valueCapacity, sizeof(uint32_t), 1, this->fp);
  fwrite(&valueLength, sizeof(uint32_t), 1, this->fp);
  fwrite(value, sizeof(V), valueLength, this->fp);
  if (valueLength < valueCapacity) {
    uint32_t nullValueLength = valueCapacity - valueLength;
    V nullValue[nullValueLength];
    std::fill(nullValue, nullValue + nullValueLength, 0);
    fwrite(nullValue, sizeof(V), nullValueLength, this->fp);
  }

  if (prevOffset == DBM::NULL_OFFSET) {
    *(this->bucket + this->calcBucketIndex(key)) = newOffset;
  }
  else {
    fseek(this->fp, prevOffset, SEEK_SET);
    fwrite(&newOffset, sizeof(uint32_t), 1, this->fp);
  }
}

template <typename V>
void DBM<V>::remove(const char* key)
{
  uint32_t prevOffset;
  uint32_t offset;
  uint32_t nextOffset;
  this->findRecordOffset(key, &prevOffset, &offset, &nextOffset);

  if (offset == DBM::NULL_OFFSET) return;

  uint32_t valueCapacity;
  fread(&valueCapacity, sizeof(uint32_t), 1, this->fp);

  this->putFreeArea(offset, DBM<V>::calcRecordSize(key, valueCapacity));

  if (prevOffset == DBM::NULL_OFFSET) {
    *(this->bucket + this->calcBucketIndex(key)) = nextOffset;
  }
  else {
    fseek(this->fp, prevOffset, SEEK_SET);
    fwrite(&nextOffset, sizeof(uint32_t), 1, this->fp);
  }
}

template <typename V>
bool DBM<V>::contains(const char* key)
{
  uint32_t prevOffset;
  uint32_t offset;
  uint32_t nextOffset;
  this->findRecordOffset(key, &prevOffset, &offset, &nextOffset);

  return (offset != DBM::NULL_OFFSET);
}

template <typename V>
void DBM<V>::loadMetaData()
{
  rewind(this->fp);

  fread(&(this->bucketLength), sizeof(uint32_t), 1, this->fp);
  this->bucket = new uint32_t[this->bucketLength];
  fread(this->bucket, sizeof(uint32_t), this->bucketLength, this->fp);

  fread(&(this->freePoolLength), sizeof(uint32_t), 1, this->fp);

  uint32_t tempFreePool[this->freePoolLength * 2];
  fread(tempFreePool, sizeof(uint32_t), this->freePoolLength * 2, this->fp);

  uint32_t index = 0;
  this->freePool->clear();
  while (index < this->freePoolLength * 2 &&
	 tempFreePool[index] != DBM::NULL_OFFSET) {
    this->freePool->push_back(std::pair<uint32_t, uint32_t>(tempFreePool[index], tempFreePool[index + 1]));
    index += 2;
  }
}

template <typename V>
uint32_t DBM<V>::calcBucketIndex(const char* key)
{
  uint32_t hash = 751;
  size_t keyLength = strlen(key);

  while (keyLength) {
    hash = hash * 37 + *((uint8_t*)key);
    --keyLength;
    ++key;
  }

  return hash % this->bucketLength;
}

template <typename V>
void DBM<V>::findRecordOffset(const char* key, uint32_t* prevOffset, uint32_t* offset, uint32_t* nextOffset)
{
  *offset = *(this->bucket + this->calcBucketIndex(key));
  *prevOffset = DBM::NULL_OFFSET;
  *nextOffset = DBM::NULL_OFFSET;

  while (*offset) {
    fseek(this->fp, *offset, SEEK_SET);

    uint32_t keyLength;
    fread(nextOffset, sizeof(uint32_t), 1, this->fp);
    fread(&keyLength, sizeof(uint32_t), 1, this->fp);

    char keyContent[keyLength];
    fread(keyContent, sizeof(char), keyLength, this->fp);

    if (strncmp(key, keyContent, keyLength) == 0) break;
    
    *prevOffset = *offset;
    *offset = *nextOffset;
  }
}

template <typename V>
void DBM<V>::saveMetaData()
{
  rewind(this->fp);

  fwrite(&(this->bucketLength), sizeof(uint32_t), 1, this->fp);
  fwrite(this->bucket, sizeof(uint32_t), this->bucketLength, this->fp);

  fwrite(&(this->freePoolLength), sizeof(uint32_t), 1, this->fp);
  
  uint32_t tempFreePool[this->freePoolLength * 2];
  std::fill(tempFreePool, tempFreePool + this->freePoolLength * 2, DBM::NULL_OFFSET);

  uint32_t index = 0;
  std::vector<std::pair<uint32_t, uint32_t> >::iterator iter = this->freePool->begin();
  while (iter != this->freePool->end()) {
    tempFreePool[index] = iter->first;
    tempFreePool[index + 1] = iter->second;
    index += 2;
    ++iter;
  }

  fwrite(tempFreePool, sizeof(uint32_t), this->freePoolLength * 2, this->fp);
}


template <typename V>
uint32_t DBM<V>::calcValueCapacity(uint32_t valueLength)
{
  uint32_t valueCapacity = DBM::INITIAL_CAPACITY;

  while (valueCapacity < valueLength) valueCapacity *= 2;

  return valueCapacity;
}

template <typename V>
uint32_t DBM<V>::getFreeArea(uint32_t requisiteSize)
{
  std::vector<std::pair<uint32_t, uint32_t> >::iterator iter = this->freePool->begin();
  while (iter != this->freePool->end()) {
    if (iter->second >= requisiteSize) {
      uint32_t offset = iter->first;
      this->freePool->erase(iter);
      return offset;
    }
    ++iter;
  }

  return DBM::NULL_OFFSET;
}

template <typename V>
void DBM<V>::putFreeArea(uint32_t offset, uint32_t size)
{
  if (this->freePool->size() >= this->freePoolLength) return;

  std::vector<std::pair<uint32_t, uint32_t> >::iterator iter = this->freePool->begin();
  while (iter != this->freePool->end()) {
    if (iter->second > size) {
      this->freePool->insert(iter, std::pair<uint32_t, uint32_t>(offset, size));
      return;
    }
    ++iter;
  }

  this->freePool->push_back(std::pair<uint32_t, uint32_t>(offset, size));
}

template <typename V>
uint32_t DBM<V>::calcRecordSize(const char* key, uint32_t valueCapacity)
{
  return sizeof(uint32_t) * 4 + sizeof(char) * strlen(key) + sizeof(V) * valueCapacity;
}

}

#endif // BB_DBM_HPP_
