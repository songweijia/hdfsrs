#include "SerializationSupport.hpp"
#include <string.h>
using namespace std;

namespace mutils {

	std::size_t to_bytes(const ByteRepresentable& b, char* v){
		return b.to_bytes(v);
	}

	std::size_t bytes_size(const ByteRepresentable& b){
		return b.bytes_size();
	}

	std::size_t to_bytes(const std::string& b, char* v){
		strcpy(v,b.c_str());
		return b.length() + 1;
	}

	std::size_t bytes_size(const std::string& b){
		return b.length() + 1;
	}
	
	void ensure_registered(ByteRepresentable& b, DeserializationManager& dm){
		b.ensure_registered(dm);
	}

	std::function<void (char const * const, std::size_t)> post_to_buffer(std::size_t &index, char * _v){
		return [&index,_v](char const * const v, std::size_t size){
			memcpy(_v + index, v, size);
			index += size;
		};
	}

	void post_object(const std::function<void (char const * const, std::size_t)>& f, const ByteRepresentable &br){
		br.post_object(f);
	}

	void post_object(const std::function<void (char const * const, std::size_t)>& f, const std::string& str) {
		f(str.c_str(), str.length() + 1);
	}
}
