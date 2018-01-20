#ifndef EXPERIMENTAL_ANNOYLIB_H
#define EXPERIMENTAL_ANNOYLIB_H

#include "../annoylib.h"

namespace experimental {

  using std::vector;
  using std::string;
  using std::pair;
  using std::numeric_limits;
  using std::make_pair;

  struct NullRandom {
    inline int flip() {
      return 0;
    }
    inline size_t index(size_t n) {
      return 0;
    }
    inline void set_seed(uint32_t seed) {}
  };

  template<typename S, typename T, typename Distance>
  class StorageInterface {
  public:
    virtual ~StorageInterface() {};
    virtual void add_item(S item, const T* v) = 0;
    virtual void get_item(S item, T *v) = 0;
    virtual void unbuild() = 0;
    virtual S get_n_items() = 0;
    virtual S get_next_id() = 0;
    virtual void verbose(bool v) = 0;
    virtual bool is_mutable() = 0;
  };

  struct MemoryStorage {
    template<typename S, typename T, typename Distance>
    class Storage : public AnnoyIndex<S, T, Distance, NullRandom>,
                    public StorageInterface<S, T, Distance> {
    public:
      typedef AnnoyIndex<S, T, Distance, NullRandom> Base;

      Storage(int f, const char *name) : Base(f) {}

      void add_item(S item, const T *v) override {
        Base::add_item(item, v);
      }

      void get_item(S item, T *v) override {
        Base::get_item(item, v);
      }

      void unbuild() override {
        Base::unbuild();
      }

      S get_n_items() override {
        return Base::get_n_items();
      }

      S get_next_id() override {
        return 0;
      }

      void verbose(bool v) override {
        Base::verbose(v);
      }

      bool is_mutable() override {
        return true;
      }

    };
  };

  struct FileStorage {
    template<typename S, typename T, typename Distance>
    class Storage : public MemoryStorage::Storage<S, T, Distance> {

    public:
      Storage(int f, const char *name)
          : MemoryStorage::Storage<S, T, Distance>(f, name) {
        MemoryStorage::Storage<S, T, Distance>::Base::load(name);
      }

      ~FileStorage() override {
        MemoryStorage::Storage<S, T, Distance>::Base::unload();
      }

      void add_item(S item, const T *v) override {
        showUpdate("You can't add an item to file storage\n");
      }

      S get_next_id() override {
        showUpdate("You can't get the next id from file storage\n");
        return -1;
      }

      bool is_mutable() override {
        return false;
      }

    };
  };

  template<typename S, typename T, typename Distance, typename Random, typename Storage>
  class AnnoyIndex : public AnnoyIndexInterface<S, T> {

  public:
    typedef Distance D;
    typedef typename D::template Node<S, T> Node;

  protected:

    typename Storage::template Storage<S, T, Distance> _storage;
    const int _f;
    Random _random;
    vector<S> _roots;
    bool _verbose;

  public:

    AnnoyIndex(int f, const char *name) : _f(f), _storage(f, name) {
      _verbose = false;
    }

    void add_item(S item, const T *w) override {
      _storage.add_item(item, w);
    }

    void build(int q) override {
      if (!_storage.is_mutable()) {
        showUpdate("You can't build using imutable storage\n");
        return;
      }
    }

    void unbuild() override {
    }

    bool save(const char *filename) override {
      return false;
    }

    // deprecated
    void unload() override {
    }

    // deprecated
    bool load(const char *filename) override {
      return false;
    }

    T get_distance(S i, S j) override {
      return 0;
    }

    void get_nns_by_item(S item, size_t n, size_t search_k, vector<S> *result,
                         vector<T> *distances) override {

    }

    void get_nns_by_vector(const T *w, size_t n, size_t search_k,
                           vector<S> *result, vector<T> *distances) override {

    }

    S get_n_items() override {
      return _storage.get_n_items();
    }

    void verbose(bool v) override {
      _verbose = v;
      _storage.verbose(v);
    }

    void get_item(S item, T *v) override {
      return _storage.get_item(item, v);
    }

    void set_seed(int seed) override {
      _random.set_seed(seed);
    }
  };


}

#endif // EXPERIMENTAL_ANNOYLIB_H
