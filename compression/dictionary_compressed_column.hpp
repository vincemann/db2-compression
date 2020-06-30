
/*! \example dictionary_compressed_column.hpp
 * This is an example of how to implement a compression technique in our framework. One has to inherit from an abstract base class CoGaDB::CompressedColumn and implement the pure virtual methods.
 */

#pragma once

#include <core/compressed_column.hpp>
#include <unordered_map>

namespace CoGaDB{
	

/*!
 *  \brief     This class represents a dictionary compressed column with type T, is the base class for all compressed typed column classes. (is it?)
 */	
template<class T>
class DictionaryCompressedColumn : public CompressedColumn<T>{
	public:
	/***************** constructors and destructor *****************/
	DictionaryCompressedColumn(const std::string& name, AttributeType db_type);
	virtual ~DictionaryCompressedColumn();

	virtual bool insert(const boost::any& new_Value);
	virtual bool insert(const T& new_value);
	template <typename InputIterator>
	bool insert(InputIterator first, InputIterator last);

	virtual bool update(TID tid, const boost::any& new_value);
	virtual bool update(PositionListPtr tid, const boost::any& new_value);	
	
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

	virtual bool store(const std::string& path);
	virtual bool load(const std::string& path);


	
	virtual T& operator[](const int index);

    protected:

    std::unordered_map<unsigned int,T> dictionary_;
    unsigned int last_key_;
};


/***************** Start of Implementation Section ******************/


    //call super constructor & init empty dictionary
	template<class T>
	DictionaryCompressedColumn<T>::DictionaryCompressedColumn(const std::string& name, AttributeType db_type) : CompressedColumn<T>(name, db_type),dictionary_(){
        this->last_key_=0;
	}

	template<class T>
	DictionaryCompressedColumn<T>::~DictionaryCompressedColumn(),dictionary_(){
        this->last_key_=0;
	}

	template<class T>
	bool DictionaryCompressedColumn<T>::insert(const boost::any& new_value){
        if(new_value.empty()) return false;
        if(typeid(T)==new_value.type()){
            T value = boost::any_cast<T>(new_value);
            return this->insert(value);
        }
		return false;
	}

	template<class T>
	bool DictionaryCompressedColumn<T>::insert(const T& value){
	    if(dictionary_.find( key )->second == value)
        unsigned int key = this->last_key_ +1;
        if(dictionary_.insert( std::make_pair( key, value ) ).second )){
            // only update last key if insert was not already present
            this->last_key_=key;
        }
        return true;
	}

	template <typename T> 
	template <typename InputIterator>
	bool DictionaryCompressedColumn<T>::insert(InputIterator , InputIterator ){
		//i dont rly know about this one
		return true;
	}

	template<class T>
	const boost::any DictionaryCompressedColumn<T>::get(TID){

		return boost::any();
	}

	template<class T>
	void DictionaryCompressedColumn<T>::print() const throw(){

	}
	template<class T>
	size_t DictionaryCompressedColumn<T>::size() const throw(){

		return 0;
	}
	template<class T>
	const ColumnPtr DictionaryCompressedColumn<T>::copy() const{

		return ColumnPtr();
	}

	template<class T>
	bool DictionaryCompressedColumn<T>::update(TID , const boost::any& ){
		return false;
	}

	template<class T>
	bool DictionaryCompressedColumn<T>::update(PositionListPtr , const boost::any& ){
		return false;		
	}
	
	template<class T>
	bool DictionaryCompressedColumn<T>::remove(TID){
		return false;	
	}
	
	template<class T>
	bool DictionaryCompressedColumn<T>::remove(PositionListPtr){
		return false;			
	}

	template<class T>
	bool DictionaryCompressedColumn<T>::clearContent(){
		return false;
	}

	template<class T>
	bool DictionaryCompressedColumn<T>::store(const std::string&){
		return false;
	}
	template<class T>
	bool DictionaryCompressedColumn<T>::load(const std::string&){
		return false;
	}

	template<class T>
	T& DictionaryCompressedColumn<T>::operator[](const int){
		static T t;
		return t;
	}

	template<class T>
	unsigned int DictionaryCompressedColumn<T>::getSizeinBytes() const throw(){
		return 0; //return values_.capacity()*sizeof(T);
	}

/***************** End of Implementation Section ******************/



}; //end namespace CogaDB

