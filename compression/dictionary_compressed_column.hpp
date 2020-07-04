
/*! \example dictionary_compressed_column.hpp
 * This is an example of how to implement a compression technique in our framework. One has to inherit from an abstract base class CoGaDB::CompressedColumn and implement the pure virtual methods.
 */

#pragma once

#include <core/compressed_column.hpp>
#include <core/column.hpp>
#include <unordered_map>

namespace CoGaDB {


/*!
 *  \brief     This class represents a dictionary compressed column with type T, is the base class for all compressed typed column classes. (is it?)
 *  Implemented as Proxy of normal uncompressed Column.
 */
    template<class T>
    class DictionaryCompressedColumn : public CompressedColumn<T> {
    public:
        /***************** constructors and destructor *****************/
        DictionaryCompressedColumn(const std::string &name, AttributeType db_type);

        virtual ~DictionaryCompressedColumn();

        virtual bool insert(const boost::any &new_Value);

        virtual bool insert(const T &new_value);

        template<typename InputIterator>
        bool insert(InputIterator first, InputIterator last);

        virtual bool update(TID tid, const boost::any &new_value);

        virtual bool update(PositionListPtr tid, const boost::any &new_value);

        virtual bool remove(TID tid);

        //assumes tid list is sorted ascending
        virtual bool remove(PositionListPtr tid);

        virtual bool clearContent();

        virtual const boost::any get(TID tid);

        //virtual const boost::any* const getRawData()=0;
        virtual void print() const throw();

        virtual size_t size() const throw();

        virtual unsigned int getSizeinBytes() const throw();

        virtual const ColumnPtr copy() const;

        virtual bool store(const std::string &path);

        virtual bool load(const std::string &path);


        virtual T &operator[](const int index);

    public:
        std::unordered_map<T, int> insert_dict_;
        std::unordered_map<int, T> at_dict_;
        int last_key_;
        Column<int> column_;
    };


/***************** Start of Implementation Section ******************/


    //call super constructor & init empty dictionary
    template<class T>
    DictionaryCompressedColumn<T>::DictionaryCompressedColumn(const std::string &name, AttributeType db_type): CompressedColumn<T>(name, db_type)
            ,insert_dict_(),at_dict_(),last_key_(),column_(name,AttributeType::INT) {
        this->insert_dict_ = std::unordered_map<T, int>();
        this->at_dict_ = std::unordered_map<int, T>();
        this->last_key_ = 0;
        this->column_ = Column<int>(name,AttributeType::INT);
    }

    template<class T>
    DictionaryCompressedColumn<T>::~DictionaryCompressedColumn()  {
        //this->insert_dict_ = std::unordered_map<T, int>();
        //this->at_dict_ = std::unordered_map<int, T>();
        //this->last_key_ = 0;
        //this->column_ = Column<int>();
    }

    template<class T>
    bool DictionaryCompressedColumn<T>::insert(const boost::any &new_value) {
        if (new_value.empty()) return false;
        if (typeid(T) == new_value.type()) {
            T value = boost::any_cast<T>(new_value);
            return this->insert(value);
        }
        return false;
    }

    template<class T>
    bool DictionaryCompressedColumn<T>::insert(const T &value) {
        std::cout << "Inserting value: " << value <<   std::endl;
        auto it = insert_dict_.find(value);
        int knownKey = -1;
        if (it != insert_dict_.end()) {
            knownKey = it->second;
        }
        int finalKey = 0;
        if (knownKey!=-1) {
            std::cout << "Key for value: " << value << " is already known: " << knownKey <<   std::endl;
            //kennen wert schon
            finalKey=knownKey;
        } else {
            //lernen wert
            finalKey = this->last_key_ + 1;
            this->last_key_ = this->last_key_ + 1;
            std::cout << "Key for value: " << value << " is not already known. Insert new key (lastkey +1) in dicts: " << finalKey <<   std::endl;
            insert_dict_.insert(std::make_pair(value,finalKey));
            at_dict_.insert(std::make_pair(finalKey, value));
        }
        return this->column_.insert(finalKey);
    }

    template<typename T>
    template<typename InputIterator>
    bool DictionaryCompressedColumn<T>::insert(InputIterator start, InputIterator end) {
        //i dont rly know about this one
        std::cout << "weird iterator insert function called " << std::endl;
        return true;
    }

    template<class T>
    const boost::any DictionaryCompressedColumn<T>::get(TID id) {
        boost::any boxedKey = this->column_.get(id);

        if(!boxedKey.empty()){
            //valid key found
            int key = boost::any_cast<int>(boxedKey);
            T value = at_dict_.at(key);
            //if(value!=null){
            std::cout << "Value found for dict key: " << key << " -> " << value << std::endl;
            return boost::any(value);
            //}else{
              //  std::cout << "No Value found for dict key " << key << std::endl;
            //}
        }else{
            std::cout << "No Key found in column for id " << id << std::endl;
        }
        return boost::any();
    }

    template<class T>
    void DictionaryCompressedColumn<T>::print() const throw() {
        //todo add compression logic
        this->column_.print();
    }

    template<class T>
    size_t DictionaryCompressedColumn<T>::size() const throw() {
        //todo add dict sizes
        return this->column_.size();
    }

    template<class T>
    const ColumnPtr DictionaryCompressedColumn<T>::copy() const {
        //todo point must point to this and not to proxied object otherwise kompression gets lost
        return ColumnPtr(new DictionaryCompressedColumn<T>(*this));
    }

    template<class T>
    bool DictionaryCompressedColumn<T>::update(TID id, const boost::any &patch) {
        //todo add compression logic
        return this->column_.update(id,patch);
    }

    template<class T>
    bool DictionaryCompressedColumn<T>::update(PositionListPtr ptr, const boost::any &patch) {
        //todo add compression logic
        return this->column_.update(ptr,patch);
    }

    template<class T>
    bool DictionaryCompressedColumn<T>::remove(TID id) {
        //todo add compression logic
        return this->column_.remove(id);
    }

    template<class T>
    bool DictionaryCompressedColumn<T>::remove(PositionListPtr ptr) {
        //todo add compression logic
        return this->column_.remove(ptr);
    }

    template<class T>
    bool DictionaryCompressedColumn<T>::clearContent() {
        //todo add compression logic
        return this->column_.clearContent();
    }

    template<class T>
    bool DictionaryCompressedColumn<T>::store(const std::string &path) {
        return this->column_.store(path);
    }

    template<class T>
    bool DictionaryCompressedColumn<T>::load(const std::string &path) {
        return this->column_.load(path);
    }

    template<class T>
    T &DictionaryCompressedColumn<T>::operator[](const int index) {
        //todo add compression logic -> this->column_[index] will return key -> return value from dict.
        int& key = this->column_[index];
        return at_dict_.at(key);
    }

    template<class T>
    unsigned int DictionaryCompressedColumn<T>::getSizeinBytes() const throw() {
        //todo add dict size
        return this->column_.getSizeinBytes(); //return values_.capacity()*sizeof(T);
    }

/***************** End of Implementation Section ******************/



}; //end namespace CogaDB

