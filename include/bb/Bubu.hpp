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

#ifndef BB_BUBU_HPP_
#define BB_BUBU_HPP_

#include <stdint.h>
#include <string>
#include <vector>
#include "bb/DBM.hpp"

namespace bb {

class Bubu
{
protected:
  DBM<uint32_t>* index;
  DBM<char>* library;

  static std::string uintToString(uint32_t uintValue);
  static void tokenizeUTF8(const char* text, bool overlap,
		    std::vector<std::string>& unigrams, 
		    std::vector<std::string>& bigrams);


public:
  Bubu();
  virtual ~Bubu();

  bool open(const char* workspaceDir);
  bool create(const char* workspaceDir);
  void close();
  std::vector<std::pair<uint32_t, uint32_t> > search(const char* query);
  void registerDoc(uint32_t docId, const char* docContent);
  void unregisterDoc(uint32_t docId);
  std::string getDocContent(uint32_t docId);
  
};

}

#endif // BB_BUBU_HPP_
