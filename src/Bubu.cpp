/**
 * Bubu.cpp
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

#include <sstream>
#include "bb/Bubu.hpp"

using bb::DBM;
using bb::Bubu;

Bubu::Bubu()
{
  this->index = new DBM<uint32_t>();
  this->library = new DBM<char>();
}

Bubu::~Bubu()
{
  this->close();
  delete this->index;
  delete this->library;
}

bool Bubu::open(const char* workspaceDir)
{
  std::string workspace(workspaceDir);
  std::string indexPath = workspace + "/bubu.idx";
  std::string libraryPath = workspace + "/bubu.lib";
  
  if (!this->index->open(indexPath.c_str()) ||
      !this->library->open(libraryPath.c_str())) {
    return false;
  }
  else {
    return true;
  }
}

bool Bubu::create(const char* workspaceDir)
{
  std::string workspace(workspaceDir);
  std::string indexPath = workspace + "/bubu.idx";
  std::string libraryPath = workspace + "/bubu.lib";
  
  if (!this->index->create(indexPath.c_str(), 100000, 10000) ||
      !this->library->create(libraryPath.c_str(), 100000, 10000)) {
    return false;
  }
  else {
    return true;
  }
}

void Bubu::close()
{
  this->index->close();
  this->library->close();
}

std::vector<std::pair<uint32_t, uint32_t> > Bubu::search(const char* query)
{
  std::vector<std::pair<uint32_t, uint32_t> > hits;
  if (query == NULL || strcmp(query, "") == 0) return hits;

  std::vector<std::string> unigrams;
  std::vector<std::string> bigrams;
  Bubu::tokenizeUTF8(query, false, unigrams, bigrams);
  uint32_t querySize = unigrams.size();
  if (querySize == 0) return hits;
  if (querySize % 2) bigrams.push_back(unigrams[querySize - 1]);

  std::vector<std::string>::iterator iter = bigrams.begin();

  uint32_t valueSize;
  uint32_t* value = this->index->get(iter->c_str(), &valueSize);
  for (uint32_t i = 0; i < valueSize; i += 2) {
    hits.push_back(std::pair<uint32_t, uint32_t>(*(value + i), *(value + i + 1)));
  }
  delete[] value;
  ++iter;

  uint32_t totalOffset = 2;
  while (iter != bigrams.end()) {
    if (hits.empty()) break;

    value = this->index->get(iter->c_str(), &valueSize);
    
    std::vector<std::pair<uint32_t, uint32_t> >::iterator hiter = hits.begin();
    while (hiter != hits.end()) {
      uint32_t docId = hiter->first;
      uint32_t offset = hiter->second + totalOffset;

      uint32_t i;
      for (i = 0; i < valueSize; i += 2) {
	if (*(value + i) == docId && *(value + i + 1) == offset) break;
      }

      hiter = (i < valueSize) ? hiter + 1 : hits.erase(hiter);
    }

    delete[] value;

    totalOffset += 2;
    ++iter;
  }

  return hits;
}

void Bubu::registerDoc(uint32_t docId, const char* docContent)
{
  if (docContent == NULL || strcmp(docContent, "") == 0) return;

  uint32_t idOffsetPair[2];

  std::vector<std::string> unigrams;
  std::vector<std::string> bigrams;
  Bubu::tokenizeUTF8(docContent, true, unigrams, bigrams);

  idOffsetPair[0] = docId;
  idOffsetPair[1] = 0;
  std::vector<std::string>::iterator unigramIter = unigrams.begin();
  while (unigramIter != unigrams.end()) {
    this->index->append(unigramIter->c_str(), idOffsetPair, 2);
    ++idOffsetPair[1];
    ++unigramIter;
  }

  idOffsetPair[0] = docId;
  idOffsetPair[1] = 0;
  std::vector<std::string>::iterator bigramIter = bigrams.begin();
  while (bigramIter != bigrams.end()) {
    this->index->append(bigramIter->c_str(), idOffsetPair, 2);
    ++idOffsetPair[1];
    ++bigramIter;
  }

  this->library->set(Bubu::uintToString(docId).c_str(), docContent, strlen(docContent));
}

void Bubu::unregisterDoc(uint32_t docId)
{
  std::string docIdString = Bubu::uintToString(docId);

  uint32_t docContentLength;
  char* docContent = this->library->get(docIdString.c_str(), &docContentLength);
  if (docContent == NULL) return;
  this->library->remove(docIdString.c_str());

  std::vector<std::string> grams;
  std::vector<std::string> bigrams;
  Bubu::tokenizeUTF8(docContent, true, grams, bigrams);
  delete[] docContent;
  grams.insert(grams.end(), bigrams.begin(), bigrams.end());

  std::vector<std::string>::iterator iter = grams.begin();
  while (iter != grams.end()) {
    uint32_t valueLength;
    uint32_t* value = this->index->get(iter->c_str(), &valueLength);
    uint32_t matchOffset = 0;
    uint32_t matchLength = 0;

    for (uint32_t i = 0; i < valueLength; i += 2) {
      if (*(value + i) == docId) {
	if (matchLength == 0) matchOffset = i;
	matchLength += 2;
      }
      else if (matchLength > 0) {
	break;
      }
    }
    
    if (matchLength > 0) {
      if (matchLength < valueLength) {
	memmove(value + matchOffset, value + matchOffset + matchLength,
		sizeof(uint32_t) * (valueLength - matchOffset - matchLength));
	this->index->set(iter->c_str(), value, valueLength - matchLength);
      }
      else {
	this->index->remove(iter->c_str());
      }
    }

    delete[] value;
    ++iter;
  }
}

std::string Bubu::getDocContent(uint32_t docId)
{
  uint32_t docContentLength;
  char* docContent = this->library->get(Bubu::uintToString(docId).c_str(), &docContentLength);
  if (docContent == NULL) {
    return std::string();
  }
  else {
    std::string docContentString(docContent, docContentLength);
    delete[] docContent;
    return docContentString;
  }
}

void Bubu::tokenizeUTF8(const char* text, bool overlap,
			std::vector<std::string>& unigrams, std::vector<std::string>& bigrams)
{
  unigrams.clear();
  bigrams.clear();

  std::string prevToken;
  std::string token;
  uint32_t size = strlen(text);

  for (uint32_t i = 0; i < size; ++i) {
    char currentByte = *(text + i);

    if ((currentByte & 0xC0) != 0x80 && !token.empty()) {
      unigrams.push_back(token);

      if (!prevToken.empty()) {
	bigrams.push_back(prevToken + token);
	if (!overlap) token.clear();
      }

      prevToken = token;
      token.clear();	
    } 

    token += currentByte;
  }
  
  unigrams.push_back(token);
  if (!prevToken.empty()) bigrams.push_back(prevToken + token);
}

std::string Bubu::uintToString(uint32_t uintValue)
{
  std::ostringstream stream;
  stream << uintValue;
  return stream.str();
}
