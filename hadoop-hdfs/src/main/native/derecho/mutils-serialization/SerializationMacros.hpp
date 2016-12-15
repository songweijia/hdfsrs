#pragma once

/**
 * Hi! SKIP down to where you see the next comment.
 */

//SERIALIZERS
#define DEFAULT_SERIALIZE1(a) std::size_t to_bytes(char* ret) const {	\
		return mutils::to_bytes(a,ret);							\
	}															\
	std::size_t bytes_size() const {									\
		return mutils::bytes_size(a);							\
	}															\
	void post_object(const std::function<void (char const * const, std::size_t)>&f ) const { \
		mutils::post_object(f,a);										\
	}
#define DEFAULT_SERIALIZE2(a,b) std::size_t to_bytes(char* ret) const {	\
		int sa = mutils::to_bytes(a,ret);						\
		return sa + mutils::to_bytes(b,ret + sa);				\
	}															\
	std::size_t bytes_size() const {									\
		return mutils::bytes_size(a) + mutils::bytes_size(b);	\
	}																	\
	void post_object(const std::function<void (char const * const, std::size_t)>&f ) const { \
		mutils::post_object(f,a);							\
		mutils::post_object(f,b);				\
	}
#define DEFAULT_SERIALIZE3(a,b,c) std::size_t to_bytes(char* ret) const {		\
		int sa = mutils::to_bytes(a,ret);								\
		int sb = mutils::to_bytes(b,ret + sa);							\
		return sa + sb + mutils::to_bytes(c,ret + sa + sb);				\
	}																	\
	std::size_t bytes_size() const {											\
		return mutils::bytes_size(a) + mutils::bytes_size(b) + mutils::bytes_size(c); \
	}																	\
	void post_object(const std::function<void (char const * const, std::size_t)>&f ) const { \
		mutils::post_object(f,a);										\
		mutils::post_object(f,b);										\
		mutils::post_object(f,c);										\
	}

#define DEFAULT_SERIALIZE4(a,b,c,d) std::size_t to_bytes(char* ret) const {		\
		int sa = mutils::to_bytes(a,ret);								\
		int sb = mutils::to_bytes(b,ret + sa);							\
		int sc = mutils::to_bytes(c,ret + sa + sb);						\
		return sa + sb + sc + mutils::to_bytes(d,ret + sa + sb + sc);	\
	}																	\
	std::size_t bytes_size() const {											\
		return mutils::bytes_size(a) + mutils::bytes_size(b) + mutils::bytes_size(c) + mutils::bytes_size(d); \
	}																	\
	void post_object(const std::function<void (char const * const, std::size_t)>&f ) const { \
		mutils::post_object(f,a);										\
		mutils::post_object(f,b);										\
		mutils::post_object(f,c);										\
		mutils::post_object(f,d);										\
	}

#define DEFAULT_SERIALIZE5(a,b,c,d,e) std::size_t to_bytes(char* ret) const {	\
		int sa = mutils::to_bytes(a,ret);								\
		int sb = mutils::to_bytes(b,ret + sa);							\
		int sc = mutils::to_bytes(c,ret + sa + sb);						\
		int sd = mutils::to_bytes(d,ret + sa + sb + sc);				\
		return sa + sb + sc + sd + mutils::to_bytes(e,ret + sa + sb + sc + sd); \
	}																	\
	std::size_t bytes_size() const {											\
		return mutils::bytes_size(a) + mutils::bytes_size(b) + mutils::bytes_size(c) + mutils::bytes_size(d) + mutils::bytes_size(e); \
	}																	\
	void post_object(const std::function<void (char const * const, std::size_t)>&f ) const { \
		mutils::post_object(f,a);										\
		mutils::post_object(f,b);										\
		mutils::post_object(f,c);										\
		mutils::post_object(f,d);										\
		mutils::post_object(f,e);										\
	}

#define DEFAULT_SERIALIZE6(a,b,c,d,e,f) std::size_t to_bytes(char* ret) const {	\
		int sa = mutils::to_bytes(a,ret);								\
		int sb = mutils::to_bytes(b,ret + sa);							\
		int sc = mutils::to_bytes(c,ret + sa + sb);						\
		int sd = mutils::to_bytes(d,ret + sa + sb + sc);				\
		int se = mutils::to_bytes(e,ret + sa + sb + sc + sd);				\
		return sa + sb + sc + sd + se + mutils::to_bytes(f,ret + sa + sb + sc + sd + se); \
	}																	\
	std::size_t bytes_size() const {											\
		return mutils::bytes_size(a) + mutils::bytes_size(b) + mutils::bytes_size(c) + mutils::bytes_size(d) + mutils::bytes_size(e) + mutils::bytes_size(f); \
	}																	\
	void post_object(const std::function<void (char const * const, std::size_t)>&fun ) const { \
		mutils::post_object(fun,a);									\
		mutils::post_object(fun,b);									\
		mutils::post_object(fun,c);									\
		mutils::post_object(fun,d);									\
		mutils::post_object(fun,e);									\
		mutils::post_object(fun,f);									\
	}

#define DEFAULT_SERIALIZE7(a,b,c,d,e,f,g) std::size_t to_bytes(char* ret) const { \
		int sa = mutils::to_bytes(a,ret);								\
		int sb = mutils::to_bytes(b,ret + sa);							\
		int sc = mutils::to_bytes(c,ret + sa + sb);						\
		int sd = mutils::to_bytes(d,ret + sa + sb + sc);				\
		int se = mutils::to_bytes(e,ret + sa + sb + sc + sd);				\
		int sf = mutils::to_bytes(f,ret + sa + sb + sc + sd + se);			\
		return sa + sb + sc + sd + se + sf + mutils::to_bytes(g,ret + sa + sb + sc + sd + se + sf); \
	}																	\
	std::size_t bytes_size() const {											\
		return mutils::bytes_size(a) + mutils::bytes_size(b) + mutils::bytes_size(c) + mutils::bytes_size(d) + mutils::bytes_size(e) + mutils::bytes_size(f) + mutils::bytes_size(g); \
	}																	\
	void post_object(const std::function<void (char const * const, std::size_t)>&fun ) const { \
		mutils::post_object(fun,a);						\
		mutils::post_object(fun,b);						\
		mutils::post_object(fun,c);						\
		mutils::post_object(fun,d);						\
		mutils::post_object(fun,e);						\
		mutils::post_object(fun,f);						\
		mutils::post_object(fun,g);						\
	}

//DESERIALIZERS
#define DEFAULT_DESERIALIZE2(Name,a)									\
	static std::unique_ptr<Name> from_bytes(mutils::DeserializationManager* p, char const * v){ \
		auto a2 = mutils::from_bytes<std::decay_t<decltype(a)> >(p,v);	\
		return make_unique<Name>(*a2);									\
	}
#define DEFAULT_DESERIALIZE3(Name,a,b)									\
	static std::unique_ptr<Name> from_bytes(mutils::DeserializationManager* p, char const * v){		\
		auto a2 = mutils::from_bytes<std::decay_t<decltype(a)> >(p,v);	\
		return make_unique<Name>(*a2,*(mutils::from_bytes<std::decay_t<decltype(b)> >(p,v + mutils::bytes_size(*a2)))); \
	}
#define DEFAULT_DESERIALIZE4(Name,a,b,c)								\
	static std::unique_ptr<Name> from_bytes(mutils::DeserializationManager* p, char const * v){					\
		auto a2 = mutils::from_bytes<std::decay_t<decltype(a)> >(p,v);	\
		auto size_a2 = mutils::bytes_size(*a2);							\
		auto b2 = mutils::from_bytes<std::decay_t<decltype(b)> >(p,v + size_a2); \
		return std::make_unique<Name>(*a2,*b2,*(mutils::from_bytes<std::decay_t<decltype(c)> >(p,v + size_a2 + mutils::bytes_size(*b2)))); \
	}
#define DEFAULT_DESERIALIZE5(Name,a,b,c,d)								\
	static std::unique_ptr<Name> from_bytes(mutils::DeserializationManager* p, char const * v){					\
		auto a2 = mutils::from_bytes<std::decay_t<decltype(a)> >(p,v);	\
		auto size_a2 = mutils::bytes_size(*a2);							\
		auto b2 = mutils::from_bytes<std::decay_t<decltype(b)> >(p,v + size_a2); \
		auto size_b2 = mutils::bytes_size(*b2);							\
		auto c2 = mutils::from_bytes<std::decay_t<decltype(c)> >(p,v + size_a2 + size_b2); \
		return std::make_unique<Name>(*a2,*b2,*c2,*(mutils::from_bytes<std::decay_t<decltype(d)> >(p,v + size_a2 + size_b2 + mutils::bytes_size(*c2)))); \
	}
#define DEFAULT_DESERIALIZE6(Name,a,b,c,d,e)								\
	static std::unique_ptr<Name> from_bytes(mutils::DeserializationManager* p, char const * v){					\
		auto a2 = mutils::from_bytes<std::decay_t<decltype(a)> >(p,v);	\
		auto size_a2 = mutils::bytes_size(*a2);							\
		auto b2 = mutils::from_bytes<std::decay_t<decltype(b)> >(p,v + size_a2); \
		auto size_b2 = mutils::bytes_size(*b2);							\
		auto c2 = mutils::from_bytes<std::decay_t<decltype(c)> >(p,v + size_a2 + size_b2); \
		auto size_c2 = mutils::bytes_size(*c2);							\
		auto d2 = mutils::from_bytes<std::decay_t<decltype(d)> >(p,v + size_a2 + size_b2 + size_c2); \
		return std::make_unique<Name>(*a2,*b2,*c2,*d2,*(mutils::from_bytes<std::decay_t<decltype(e)> >(p,v + size_a2 + size_b2 + size_c2 + mutils::bytes_size(*d2)))); \
	}
#define DEFAULT_DESERIALIZE7(Name,a,b,c,d,e,f)							\
	static std::unique_ptr<Name> from_bytes(mutils::DeserializationManager* p, char const * v){					\
		auto a2 = mutils::from_bytes<std::decay_t<decltype(a)> >(p,v);	\
		auto size_a2 = mutils::bytes_size(*a2);							\
		auto b2 = mutils::from_bytes<std::decay_t<decltype(b)> >(p,v + size_a2); \
		auto size_b2 = mutils::bytes_size(*b2);							\
		auto c2 = mutils::from_bytes<std::decay_t<decltype(c)> >(p,v + size_a2 + size_b2); \
		auto size_c2 = mutils::bytes_size(*c2);							\
		auto d2 = mutils::from_bytes<std::decay_t<decltype(d)> >(p,v + size_a2 + size_b2 + size_c2); \
		auto size_d2 = mutils::bytes_size(*d2);							\
		auto e2 = mutils::from_bytes<std::decay_t<decltype(e)> >(p,v + size_a2 + size_b2 + size_c2 + size_d2); \
		return std::make_unique<Name>(*a2,*b2,*c2,*d2,*e2,*(mutils::from_bytes<std::decay_t<decltype(f)> >(p,v + size_a2 + size_b2 + size_c2 + size_d2 + mutils::bytes_size(*e2)))); \
	}

#define DEFAULT_DESERIALIZE8(Name,a,b,c,d,e,f,g)							\
	static std::unique_ptr<Name> from_bytes(mutils::DeserializationManager* p, char const * v){					\
		auto a2 = mutils::from_bytes<std::decay_t<decltype(a)> >(p,v);	\
		auto size_a2 = mutils::bytes_size(*a2);							\
		auto b2 = mutils::from_bytes<std::decay_t<decltype(b)> >(p,v + size_a2); \
		auto size_b2 = mutils::bytes_size(*b2);							\
		auto c2 = mutils::from_bytes<std::decay_t<decltype(c)> >(p,v + size_a2 + size_b2); \
		auto size_c2 = mutils::bytes_size(*c2);							\
		auto d2 = mutils::from_bytes<std::decay_t<decltype(d)> >(p,v + size_a2 + size_b2 + size_c2); \
		auto size_d2 = mutils::bytes_size(*d2);							\
		auto e2 = mutils::from_bytes<std::decay_t<decltype(e)> >(p,v + size_a2 + size_b2 + size_c2 + size_d2); \
		auto size_e2 = mutils::bytes_size(*e2);							\
		auto f2 = mutils::from_bytes<std::decay_t<decltype(f)> >(p,v + size_a2 + size_b2 + size_c2 + size_d2 + size_e2); \
		return std::make_unique<Name>(*a2,*b2,*c2,*d2,*e2,*f2,*(mutils::from_bytes<std::decay_t<decltype(g)> >(p,v + size_a2 + size_b2 + size_c2 + size_d2 + size_e2 + mutils::bytes_size(*f2)))); \
	}
	
#define DEFAULT_SERIALIZE_IMPL2(count, ...) DEFAULT_SERIALIZE ## count (__VA_ARGS__)
#define DEFAULT_SERIALIZE_IMPL(count, ...) DEFAULT_SERIALIZE_IMPL2(count, __VA_ARGS__)
#define DEFAULT_SERIALIZE(...) DEFAULT_SERIALIZE_IMPL(VA_NARGS(__VA_ARGS__), __VA_ARGS__)


#define DEFAULT_DESERIALIZE_IMPL2(count, ...) DEFAULT_DESERIALIZE ## count (__VA_ARGS__)
#define DEFAULT_DESERIALIZE_IMPL(count, ...) DEFAULT_DESERIALIZE_IMPL2(count, __VA_ARGS__)
#define DEFAULT_DESERIALIZE(...) DEFAULT_DESERIALIZE_IMPL(VA_NARGS(__VA_ARGS__), __VA_ARGS__)


/**
 * THIS (below) is the only user-facing macro in this file.
 * It's for automatically generating basic serialization support.
 * plop this macro inside the body of a class which extends 
 * ByteRepresentable, providing the name of the class (that you plopped this into)
 * as the first argument and the name of the class's fields as the remaining arguments.
 * Right now we only support up to seven fields; adding more support is easy, just ask if
 * you need.
 *
 * MAJOR CAVEAT: This macro assumes that there is a constructor
 * which takes all the class members (in the order listed). 
 * it's fine if this is a private constructor, but it needs to exist.
 * 
 */

#define DEFAULT_SERIALIZATION_SUPPORT(CLASS_NAME,CLASS_MEMBERS...)		\
        DEFAULT_SERIALIZE(CLASS_MEMBERS) DEFAULT_DESERIALIZE(CLASS_NAME,CLASS_MEMBERS)   \
    void ensure_registered(mutils::DeserializationManager&){}


