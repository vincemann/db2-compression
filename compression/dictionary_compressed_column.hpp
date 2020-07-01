
/*! \example dictionary_compressed_column.hpp
 * This is an example of how to implement a compression technique in our framework. One has to inherit from an abstract base class CoGaDB::CompressedColumn and implement the pure virtual methods.
 */

#pragma once

#include <core/compressed_column.hpp>
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

    protected:

        std::unordered_map<T, int> insert_dict_;
        std::unordered_map<int, T> at_dict_;
        int last_key_;
        Column<int> column_;
    };


/***************** Start of Implementation Section ******************/


    //call super constructor & init empty dictionary
    template<class T>
    DictionaryCompressedColumn<T>::DictionaryCompressedColumn(const std::string &name, AttributeType db_type)
            : CompressedColumn<T>(name, db_type), insert_dict_(),at_dict_() {
        this->last_key_ = 0;
        this->column_ = new Column(name,db_type);
    }

    template<class T>
    DictionaryCompressedColumn<T>::~DictionaryCompressedColumn(), insert_dict_(),at_dict_() {
        this->last_key_ = 0;
        this->column_ = new Column();
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
        int knownKey = insert_dict_.at(value);
        int finalKey = 0;
        if (knownKey!=0) {
            //kennen wert schon
            finalKey=knownKey;
        } else {
            //lernen wert
            finalKey = this->last_key_ + 1;
            insert_dict_.insert(std::make_pair(value,finalKey));
            at_dict_.insert(std::make_pair(finalKey, value));
        }
        return this->column_->insert(key);
    }

    template<typename T>
    template<typename InputIterator>
    bool DictionaryCompressedColumn<T>::insert(InputIterator start, InputIterator end) {
        //i dont rly know about this one
        return true;
    }

    template<class T>
    const boost::any DictionaryCompressedColumn<T>::get(TID id) {
        int key = boost::any_cast<int>(this->column_->get(id));
        if(!key.empty()){
            //valid key found
            T value = at_dict_.at(key);
            if(value){
                return boost::any(value);
            }else{
                std::cout << "No Value found for dict key " << key << std::endl;
            }
        }
        return boost::any();
    }

    template<class T>
    void DictionaryCompressedColumn<T>::print() const throw() {
        this->column_->print();
    }

    template<class T>
    size_t DictionaryCompressedColumn<T>::size() const throw() {
        return this->column_->size();
    }

    template<class T>
    const ColumnPtr DictionaryCompressedColumn<T>::copy() const {
        return this->column_->copy();
    }

    template<class T>
    bool DictionaryCompressedColumn<T>::update(TID, const boost::any &) {
        return false;
    }

    template<class T>
    bool DictionaryCompressedColumn<T>::update(PositionListPtr, const boost::any &) {
        return false;
    }

    template<class T>
    bool DictionaryCompressedColumn<T>::remove(TID) {
        return false;
    }

    template<class T>
    bool DictionaryCompressedColumn<T>::remove(PositionListPtr) {
        return false;
    }

    template<class T>
    bool DictionaryCompressedColumn<T>::clearContent() {
        return false;
    }

    template<class T>
    bool DictionaryCompressedColumn<T>::store(const std::string &) {
        return false;
    }

    template<class T>
    bool DictionaryCompressedColumn<T>::load(const std::string &) {
        return false;
    }

    template<class T>
    T &DictionaryCompressedColumn<T>::operator[](const int index) {
        return this->column_[index];
    }

    template<class T>
    unsigned int DictionaryCompressedColumn<T>::getSizeinBytes() const throw() {
        return this->column_->getSizeinBytes(); //return values_.capacity()*sizeof(T);
    }

/***************** End of Implementation Section ******************/



}; //end namespace CogaDB

