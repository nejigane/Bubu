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
