#pragma once
#include "mutils.hpp"
#include "type_utils.hpp"
#include "SerializationMacros.hpp"
#include "macro_utils.hpp"
#include <vector>
#include <cstring>

//BEGIN DECLARATIONS AND USAGE INFORMATION

namespace mutils{

	//forward declaration
	struct DeserializationManager;

	/** 
	 * A non-POD type which wishes to mark itself byte representable should extend this class.
	 * Intended to use to convert classes to and from contiguous sequences of bytes.
	 */
	struct ByteRepresentable {

		/**
		 * Write this class's marshalled representation into the array found at v.
		 * assume v has at least bytes_size() of free memory available; behavior
		 * is undefined otherwise.
		 * 
		 * Returns number of bytes written, which should be the same as bytes_size().
		 *
		 * NOTE: it is recommended that users not call this directly, and prefer
		 * to use mutils::to_bytes(T,v) instead.
		 */
		virtual std::size_t to_bytes(char* v) const = 0;

		/**
		 * Pass a pointer to a buffer containing this class's marshalled representation 
		 * into the function f.  This pointer is not guaranteed to live beyond the duration
		 * of the call to f, so make a copy if you need to keep it around.
		 *
		 * NOTE: it is recommended that users not call this directly, and prefer
		 * to use mutils::post_object(f,T) instead.
		 */
		virtual void post_object(const std::function<void (char const * const,std::size_t)>&) const = 0;

		/**
		 * the size of the marshalled representation of this object. 
		 * useful when allocating arrays in which to store this object.
		 *
		 * NOTE: it is recommended that users not call this directly, and prefer
		 * to use mutils::bytes_size(T,v) instead.
		 */
		virtual std::size_t bytes_size() const = 0;

		/**
		 * If this object requires context in order to correctly 
		 * deserialize, this method will associate that context
		 * with the provided DeserializationManager.  Many objects
		 * will not require context, and so can leave this function 
		 * empty.
		 */
		virtual void ensure_registered(DeserializationManager&) = 0;
		virtual ~ByteRepresentable(){}

		/**
		 * from_bytes takes the DeserializationManager which manages this object's context
		 * (or nullptr, if this object does not require a context), a byte array of size
		 * at least bytes_size(), and returns a new heap-allocated instance of that object.
		 *
		 * NOTE: it is recommended that users not call this directly, and prefer
		 * to use mutils::from_bytes<T>(DeserializationManager*,v) instead.
		 */
		//needs to exist, but can't declare virtual statics
		//virtual static std::unique_ptr<T> from_bytes(DeserializationManager *p, char *v) const  = 0;
	};

	/**
	 * If a class which implements ByteRepresentable requires a context in order
	 * to correctly deserialize, that context should be represented as a class that extends
	 * RemoteDeserializationContext.  If no context is required, then this class is
	 * not necessary. 
	 */
	struct RemoteDeserializationContext{
		DeserializationManager* this_mgr{nullptr};
		RemoteDeserializationContext(const RemoteDeserializationContext&) = delete;
		virtual ~RemoteDeserializationContext(){}
		RemoteDeserializationContext(){}
	};

	//a pointer to a RemoteDeserializationContext. This exists
	//so that I can switch it out with smart pointer types when
	//debugging stuff.
	using RemoteDeserializationContext_p =
                RemoteDeserializationContext*;

	//a vector of RemoteDeserializationContext*.
	//it's got a using declaration for the same reason
	//as the above.
	using RemoteDeserialization_v =
		std::vector<RemoteDeserializationContext_p>;

	/**
	 * The manager for any RemoteDeserializationContexts.
	 * Don't subclass this; rather construct it with any context managers
	 * you need as arguments to it.
	 * /be sure to have a pointer to this on hand whenever you need to deserialize
	 * something. If you're dead certain you never need a deserialization
	 * context, then you can not use this at all and just pass null
	 * to from_bytes in place of this.
	 */
	struct DeserializationManager {

		/**
		 * Various registered managers. Please note that this class
		 * does *not* own these pointers; you need to keep them 
		 * managed somewhere else. Also ensure lifetime of this
		 * class is shorter than or the same as those registered
		 * contexts.
		 */
		const RemoteDeserialization_v registered_v;
		DeserializationManager(RemoteDeserialization_v rv):registered_v(rv){
			for (auto &r : registered_v) r->this_mgr = this;
		}
		DeserializationManager(const DeserializationManager&) = delete;

		/**
		 * Lookup the context registered at this DeserializationManager 
		 * whose type is T.  Note this means we assume that types uniquely 
		 * identify contexts. 
		 */
		template<typename T>
		T& mgr() {
			for (auto& candidate : registered_v){
				if (dynamic_cast<T*>(candidate))
					return dynamic_cast<T&>(*candidate);
				
			}
			assert(false && "Error: no registered manager exists");
		}

		/**
		 * As the above, but const. 
		 */
		template<typename T>
		const T& mgr() const {
			for (auto& candidate : registered_v){
				if (dynamic_cast<T*>(candidate))
					return dynamic_cast<T&>(*candidate);
			}
			assert(false && "Error: no registered manager exists");
		}

		/**
		 * checks to see if a context of type T has been 
		 * registered with this DeserializationManager.
		 */
		template<typename T>
		bool registered() const {
			for (auto& candidate : registered_v){
                                if (dynamic_cast<T const *>(candidate))
					return true;
			}
			return false;
		}
	};

	/**
     * Just calls sizeof(T)
     */
    template<typename T, restrict2(std::is_pod<T>::value)>
    auto bytes_size(const T&){
        return sizeof(T);
    }

    /**
     * calls b.bytes_size() when b is a ByteRepresentable;
     * calls sizeof(decay_t<decltype(b)>) when b is a POD;
     * custom logic is implemented for some STL types.
     */
    std::size_t bytes_size(const ByteRepresentable& b);

    /**
     * effectively strlen().
     */
    std::size_t bytes_size(const std::string& b);

    /**
     * all of the elements of this vector, plus one int
     * for the number of elements.
     */
    template<typename T>
    std::size_t bytes_size (const std::vector<T> &v){
        if (std::is_pod<T>::value)
            return v.size() * bytes_size(v.back()) + sizeof(int);
        else {
            int accum = 0;
            for (auto &e : v) accum += bytes_size(e);
            return accum + sizeof(int);
        }
    }

    /**
     * sums the size of both pair elements
     */
    template<typename T, typename V>
    std::size_t bytes_size (const std::pair<T,V> &pair){
        return bytes_size(pair.first) + bytes_size(pair.second);
    }

    template<typename T>
    std::size_t bytes_size(const std::set<T>& s){
        int size = sizeof(int);
        for (auto &a : s) {
            size += bytes_size(a);
        }
        return size;
    }

    /**
     * In-place serialization is also sometimes possible.
     * This will take a function that expects buffers to be posted,
     * and will post the object (potentially in multiple buffers)
     * via repeated calls to the function
     */
    template<typename BR>
    std::enable_if_t<std::is_pod<BR>::value>
    post_object(const std::function<void (char const * const, std::size_t)>& f, const BR& br){
        f((char*)&br,sizeof(BR));
    }

    void post_object(const std::function<void (char const * const, std::size_t)>& f, const ByteRepresentable& br);

	/**
	 * Calls b.ensure_registered(dm) when b is a ByteRepresentable;
	 * returns true when b is POD.
	 */
	void ensure_registered(ByteRepresentable& b, DeserializationManager& dm);

	/**
	 * Calls b.ensure_registered(dm) when b is a ByteRepresentable;
	 * returns true when b is POD.
	 */
	template<typename T, restrict(std::is_pod<T>::value)>
	void ensure_registered(const T&, DeserializationManager&){}

	/**
	 * calls b.to_bytes(v) when b is a ByteRepresentable;
	 * calls std::memcpy() when b is POD.  Custom logic 
	 * is implemented for some STL types.  When ubuntu
	 * gets GCC5.0 or better, this will also work if
	 * b is trivially copyable.
	 */
	std::size_t to_bytes(const ByteRepresentable& b, char* v);

	/**
	 * extracts the C string (char*) equivalent to this 
	 * std::string and stores it in v
	 */
	std::size_t to_bytes(const std::string& b, char* v);

	/**
	 * Calls T::from_bytes(ctx,v) when T is a ByteRepresentable. 
	 * uses std::memcpy() when T is a POD.  
	 * custom logic is implemented for some STL types.
	 */
	template<typename T>
	std::enable_if_t<std::is_base_of<ByteRepresentable CMA T>::value,
					 std::unique_ptr<T> > from_bytes(DeserializationManager* ctx, char const *v){
		return T::from_bytes(ctx,v);
	}

	/**
	 * Calls T::from_bytes(ctx,v) when T is a ByteRepresentable. 
	 * uses std::memcpy() when T is a POD.  
	 * custom logic is implemented for some STL types.
	 */
	template<typename T>
	std::enable_if_t<std::is_pod<T>::value
					 ,std::unique_ptr<std::decay_t<T> > > from_bytes(DeserializationManager*, char const *v);

	/** 
	 * The "marshalled" type is a wrapper for already-serialized types; 
	 */

	struct marshalled : public ByteRepresentable{
		const std::size_t size;
		char const * const data;

		marshalled(decltype(size) size, decltype(data) data)
			:size(size),data(data){}

		std::size_t to_bytes(char* v) const {
			std::memcpy(v,data,size);
			return size;
		}
		std::size_t bytes_size() const {
			return size;
		}
		void ensure_registered(DeserializationManager&){}

		template<typename DSM>
		static std::unique_ptr<marshalled>
		from_bytes(DSM const * const, char const * const){
			static_assert(std::is_same<DSM,void>::value &&
						  !std::is_same<DSM,void>::value,
						  "Do not deserialize into a marshalled. please."
				);
			return nullptr;
		}
	};


	/**
	 * Serialization is also implemented for the following STL types:
	 * vector
	 * pair
	 * string
	 * set
	 */

	//end forward-declaring; everything past this point is implementation,
	//and not essential to understanding  the interface.

	std::function<void (char const * const, std::size_t)> post_to_buffer(std::size_t &index, char * _v);


	//post_object definitions -- must come before to_bytes definitions that use them
	void post_object(const std::function<void (char const * const, std::size_t)>& f, const std::string& str);

	template<typename T>
    void post_object(const std::function<void (char const * const, std::size_t)>& f, const std::vector<T>& vec){
        int size = vec.size();
        f((char*)&size,sizeof(size));
        if (std::is_pod<T>::value){
            std::size_t size = vec.size() * bytes_size(vec.back());
            f((char*) vec.data(), size);
        }
        else{
            for (const auto &e : vec){
                post_object(f,e);
            }
        }
    }

    template<typename T>
    void post_object(const std::function<void (char const * const, std::size_t)>& f, const std::set<T>& s){
        int size = s.size();
        f((char*)&size,sizeof(size));
        for (const auto &a : s){
            post_object(f,a);
        }
    }

    template<typename T, typename V>
    void post_object(const std::function<void (char const * const, std::size_t)>& f, const std::pair<T,V>& pair){
        post_object(f,pair.first);
        post_object(f,pair.second);
    }

    //end post_object section

    //to_bytes definitions -- these must come after bytes_size and post_object definitions
    template<typename T, restrict(std::is_pod<T>::value)>
    auto to_bytes(const T &t, char* v){
        auto res = std::memcpy(v,&t,sizeof(T));
        assert(res);
        return sizeof(T);
    }

	template<typename T>
	std::size_t to_bytes(const std::vector<T> &vec, char* v){
		auto size = bytes_size(vec);
		std::size_t index = 0;
		post_object(post_to_buffer(index,v), vec);
		return size;

	}

	template<typename T, typename V>
	std::size_t to_bytes(const std::pair<T,V> &pair, char*){
		std::size_t index{0};
		post_to_buffer(index,pair.first);
		post_to_buffer(index,pair.second);
		return bytes_size(pair);
	}

	template<typename T>
    std::size_t to_bytes(const std::set<T>& s, char* _v){
        std::size_t index{0};
        auto size = bytes_size(s);
        post_object(post_to_buffer(index,_v),s);
        return size;

    }
	//end to_bytes section

	//ensure_registered definitions -- these could go anywhere since they don't depend on any other functions
	template<typename T>
	void ensure_registered(const std::vector<T>& v, DeserializationManager& dm){
		for (auto &e : v) ensure_registered(e,dm);
	}

	template<typename L, typename R>
	void ensure_registered(const std::pair<L,R>& v, DeserializationManager& dm){
		ensure_registered(v.first,dm);
		ensure_registered(v.second,dm);
	}

    template<typename T>
    void ensure_registered(const std::set<T>& v, DeserializationManager& dm){
        for (auto &e : v) ensure_registered(e,dm);
    }
    //end ensure_registered section

	//from_bytes definitions
	template<typename T>
	std::enable_if_t<std::is_pod<T>::value
					 ,std::unique_ptr<std::decay_t<T> > > from_bytes(DeserializationManager*, char const *v){
		using T2 = std::decay_t<T>;
		if (v) {
			auto t = std::make_unique<T2>(*(T2*)v);
			//std::memcpy(t.get(),v,sizeof(T));
			return std::move(t);
		}
		else return nullptr;
	}

	template<typename T, typename P>
	std::unique_ptr<T> from_bytes_stupid(P* p, T*, char const * v) {
		return from_bytes<T>(p,v);
	}

	template<typename>
	struct is_pair : std::false_type {};

	template<typename T, typename U>
	struct is_pair<std::pair<T,U> > : std::true_type {};

	template<typename>
	struct is_string : std::false_type {};

	template<>
	struct is_string<std::string> : std::true_type {};

	template<>
	struct is_string<const std::string> : std::true_type {};
	
	template<typename T>
	std::unique_ptr<type_check<is_string,T> > from_bytes(DeserializationManager*, char const *v){
		assert(v);
		return std::make_unique<T>(v);
	}
	
	template<typename T, typename P>
	std::unique_ptr<type_check<is_set,T> > from_bytes(P* ctx, char* const _v) {
		int size = ((int*)_v)[0];
		char* v = _v + sizeof(int);
		auto r = std::make_unique<std::set<typename T::key_type> >();
		for (int i = 0; i < size; ++i){
			auto e = from_bytes<typename T::key_type>(ctx,v);
			v += bytes_size(*e);
			r->insert(*e);
		}
		return std::move(r);
	}

	template<typename T, typename P>
	std::unique_ptr<type_check<is_pair,T > > from_bytes(P* ctx, char const * v){
		using ft = typename T::first_type;
		using st = typename T::second_type;
		auto fst = from_bytes<ft>(ctx,v);
		return std::make_unique<std::pair<ft,st> >
			(*fst, *from_bytes<st>(ctx,v + bytes_size(*fst)));
	}

	template<typename T, typename P>
	std::enable_if_t<is_vector<T>::value,std::unique_ptr<T> > from_bytes(P* ctx, char const * v){
		using member = typename T::value_type;
		if (std::is_pod<typename T::value_type>::value){
			member const * const start = (member*) (v + sizeof(int));
			const int size = ((int*)v)[0];
			return std::unique_ptr<T>{new T{start, start + size}};
		}
		else{
			int size = ((int*)v)[0];
			auto* v2 = v + sizeof(int);
			int per_item_size = -1;
			std::unique_ptr<std::vector<member> > accum{new T()};
			for(int i = 0; i < size; ++i){
				std::unique_ptr<member> item = from_bytes<typename T::value_type>(ctx,v2 + (i * per_item_size));
				if (per_item_size == -1)
					per_item_size = bytes_size(*item);
				accum->push_back(*item);
			}
			return accum;
		}
	}
}
