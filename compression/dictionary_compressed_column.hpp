
/*! \example dictionary_compressed_column.hpp
 * This is an example of how to implement a compression technique in our framework. One has to inherit from an abstract base class CoGaDB::CompressedColumn and implement the pure virtual methods.
 */

#pragma once

#include <core/compressed_column.hpp>
#include <core/column.hpp>

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

        virtual void serialize(boost::archive::binary_oarchive& out);

        virtual void deserialize(boost::archive::binary_iarchive& in);

        virtual bool insert(const boost::any &new_Value);

        virtual bool insert(const T &new_value);

        virtual bool isOfTypeT(const boost::any &new_value);

        virtual int getKeyFor(const T &value, bool* known);

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
        std::map<T, int> insert_dict_;
        std::map<int, T> at_dict_;
        std::map<int, int> value_count_dict_;
        int last_key_;
        Column<int> column_;
    };


/***************** Start of Implementation Section ******************/


    //call super constructor & init empty dictionary
    template<class T>
    DictionaryCompressedColumn<T>::DictionaryCompressedColumn(const std::string &name, AttributeType db_type): CompressedColumn<T>(name, db_type)
            ,insert_dict_(),at_dict_(),value_count_dict_(),last_key_(),column_(name,AttributeType::INT) {
        this->insert_dict_ = std::map<T, int>();
        this->at_dict_ = std::map<int, T>();
        this->value_count_dict_ = std::map<int, int>();
        this->last_key_ = 0;
        this->column_ = Column<int>(name,AttributeType::INT);
    }

    template<class T>
    DictionaryCompressedColumn<T>::~DictionaryCompressedColumn()  {
        //this->insert_dict_ = std::map<T, int>();
        //this->at_dict_ = std::map<int, T>();
        //this->last_key_ = 0;
        //this->column_ = Column<int>();
    }

    template<class T>
    bool DictionaryCompressedColumn<T>::insert(const boost::any &new_value) {
        if(isOfTypeT(new_value)){
            T value = boost::any_cast<T>(new_value);
            return this->insert(value);
        }else{
            return false;
        }
    }

    template<class T>
    bool DictionaryCompressedColumn<T>::isOfTypeT(const boost::any &new_value){
        if (new_value.empty()) return false;
        if (typeid(T) == new_value.type()) {
            return true;
        }
        return false;
    }

    template<class T>
    bool DictionaryCompressedColumn<T>::insert(const T &value) {
        bool* known = new bool;
        *known = 0;
        int key = getKeyFor(value,known);
        bool inserted = this->column_.insert(key);
        //increment value count for already known value
        if(inserted && known){
            this->value_count_dict_[key]=this->value_count_dict_[key]+1;
        }
        return inserted;
    }

    template<class T>
    int DictionaryCompressedColumn<T>::getKeyFor(const T &value, bool* known){
        auto it = insert_dict_.find(value);
        int key = -1;
        if (it != insert_dict_.end()) {
            key = it->second;
        }
        if (key!=-1) {
            std::cout << "Key for value: " << value << " is already known: " << key <<   std::endl;
            *known = 1;
            //we already know the value
        } else {
            *known = 0;
            //lernen wert
            key = this->last_key_ + 1;
            this->last_key_ = this->last_key_ + 1;
            std::cout << "Key for value: " << value << " is not already known. Insert new key (lastkey +1) in dicts: " << key <<   std::endl;
            insert_dict_.insert(std::make_pair(value,key));
            at_dict_.insert(std::make_pair(key, value));
            value_count_dict_.insert(std::make_pair(key, 1));
        }
        return key;
    }

    template<typename T>
    template<typename InputIterator>
    bool DictionaryCompressedColumn<T>::insert(InputIterator start, InputIterator end) {
        bool inserted = true;
        for (auto it = start; it != end; ++it) {
            inserted = insert(*it);
        }
        return inserted;
    }

    template<class T>
    const boost::any DictionaryCompressedColumn<T>::get(TID id) {
        boost::any anyKey = this->column_.get(id);
        if(!anyKey.empty()){
            //valid key found
            int key = boost::any_cast<int>(anyKey);
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
        this->column_.print();
    }

    template<class T>
    size_t DictionaryCompressedColumn<T>::size() const throw() {
        return this->column_.size();
    }

    template<class T>
    const ColumnPtr DictionaryCompressedColumn<T>::copy() const {
        return ColumnPtr(new DictionaryCompressedColumn<T>(*this));
    }

    template<class T>
    bool DictionaryCompressedColumn<T>::update(TID id, const boost::any &patch) {
        if(isOfTypeT(patch)){
            bool* known = new bool;
            *known = 0;
            T value = boost::any_cast<T>(patch);
            int key = this->getKeyFor(value,known);
            return this->column_.update(id,key);
        }else{
            return false;
        }
    }

    template<class T>
    bool DictionaryCompressedColumn<T>::update(PositionListPtr ptr, const boost::any &value) {
        if (value.empty() || typeid(T) != value.type()) {
            return false;
        }
        for (auto it = ptr->begin(); it != ptr->end(); ++it) {
            this->update(*it, value);
        }
        return true;
    }

    template<class T>
    bool DictionaryCompressedColumn<T>::remove(TID id) {
        int key = this->column_[id];
        int value_count = this->value_count_dict_[key];
        if(value_count==1){
            this->value_count_dict_.erase(key);
            T val = this->at_dict_[key];
            this->at_dict_.erase(key);
            this->insert_dict_.erase(val);

        }else{
            this->value_count_dict_[key]=this->value_count_dict_[key]-1;
        }
        return this->column_.remove(id);
    }

    template<class T>
    bool DictionaryCompressedColumn<T>::remove(PositionListPtr ptr) {
        if (!ptr || ptr->empty()) {
            return false;
        }

        for (PositionList::reverse_iterator rit = ptr->rbegin(); rit != ptr->rend(); ++rit) {
            this->remove(*rit);
        }

        return true;

    }

    template<class T>
    bool DictionaryCompressedColumn<T>::clearContent() {
        this->insert_dict_.clear();
        this->at_dict_.clear();
        this->last_key_=0;
        return this->column_.clearContent();
    }


    //map serialize and deserialize
    template<class T>
    void DictionaryCompressedColumn<T>::serialize(boost::archive::binary_oarchive& out) {
        out << this->insert_dict_.size();
        for (auto const& p: insert_dict_) { out << p.first << p.second; }
        out << this->at_dict_.size();
        for (auto const& p: at_dict_) { out << p.first << p.second; }
    }

    template<class T>
    void DictionaryCompressedColumn<T>::deserialize(boost::archive::binary_iarchive& in) {
        size_t size = 0;
        in >> size;

        for (size_t i = 0; i != size; ++i) {
            T key;
            int value;
            in >> key >> value;
            this->insert_dict_[key] = value;
        }

        size_t size2 = 0;
        in >> size2;

        for (size_t i = 0; i != size; ++i) {
            int key;
            T value;
            in >> key >> value;
            this->at_dict_[key] = value;
        }

    }

    template<class T>
    bool DictionaryCompressedColumn<T>::store(const std::string &path_) {
        //return this->column_.store(path);
        //string path("data/");
        std::string path(path_);
        path += "/";
        path += this->name_+"-meta";
        //std::cout << "Writing Column " << this->getName() << " to File " << path << std::endl;
        std::ofstream outfile (path.c_str(),std::ios_base::binary | std::ios_base::out);
        boost::archive::binary_oarchive oa(outfile);

        oa << last_key_;
        serialize(oa);


        outfile.flush();
        outfile.close();
        return this->column_.store(path_);
    }

    template<class T>
    bool DictionaryCompressedColumn<T>::load(const std::string &path_) {
        std::string path(path_);
        //std::cout << "Loading column '" << this->name_ << "' from path '" << path << "'..." << std::endl;
        //string path("data/");
        path += "/";
        path += this->name_+"-meta";

        //std::cout << "Opening File '" << path << "'..." << std::endl;
        std::ifstream infile (path.c_str(),std::ios_base::binary | std::ios_base::in);
        boost::archive::binary_iarchive ia(infile);

        ia >> last_key_;
        deserialize(ia);

        infile.close();


        return this->column_.load(path_);
    }


    template<class T>
    T &DictionaryCompressedColumn<T>::operator[](const int index) {
        //std::cout << "Array operator called with index: " << index << std::endl;
        int& key = this->column_[index];
        //std::cout << "Found key for this index: " << key << std::endl;
        return at_dict_.at(key);
    }

    template<class T>
    unsigned int DictionaryCompressedColumn<T>::getSizeinBytes() const throw() {
        return this->column_.getSizeinBytes(); //return values_.capacity()*sizeof(T);
    }

/***************** End of Implementation Section ******************/



}; //end namespace CogaDB

