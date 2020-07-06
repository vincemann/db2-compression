

/*! /brief DeltaCodingCompressedColumn.hpp

 * This is a compression technique in our framework. One has to inherit from an abstract base class CoGaDB::CompressedColumn and implement the pure virtual methods.

 */



#pragma once


#include <core/compressed_column.hpp>

#include <core/column.hpp>


namespace CoGaDB {


/*!

 *  \brief     This class represents a DeltaCodingCompressedColumn with type T, is the base class for all compressed typed column classes.

 */

    template<class T>

    class DeltaCodingCompressedColumn : public CompressedColumn<T> {

    public:

        /***************** constructors and destructor *****************/

        DeltaCodingCompressedColumn(const std::string &name, AttributeType db_type);

        virtual ~DeltaCodingCompressedColumn();


        virtual T &decompress(TID id);


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

        Column <T> column_;

        T last_value_;


    };


/***************** Start of Implementation Section ******************/



    template<class T>

    DeltaCodingCompressedColumn<T>::DeltaCodingCompressedColumn(const std::string &name, AttributeType db_type)
            : CompressedColumn<T>(name, db_type),

              column_(name, db_type), last_value_() {

        this->column_ = Column<T>(name, db_type);

    }


    template<class T>

    DeltaCodingCompressedColumn<T>::~DeltaCodingCompressedColumn() {


    }


    template<class T>

    bool DeltaCodingCompressedColumn<T>::insert(const boost::any &value) {

        return this->column_.insert(value);

    }


    template<class T>

    bool DeltaCodingCompressedColumn<T>::insert(const T &value) {

        // (1. sortieren)

        // 2. wenn keine vorherige zahl, dann zahl speichern ansonsten :

        //    differenz zu vorherigen zahl speichern

        if (column_.getContent().empty()) {

            std::cout << std::endl << "Saving first entry: " << value << std::endl;

            this->column_.insert(value);

            this->last_value_ = value;

        } else {

            //std::cout << "last value: " << last_value_ << std::endl;

            std::cout << std::endl << "Saving entry delta: " << value - this->last_value_ << " for value : " << value
                      << " id : " << column_.size() << std::endl;

            this->column_.insert(value - this->last_value_);

            this->last_value_ = value;

        }



        // write whole column

        std::cout << std::endl;

        for (long unsigned int i = 0; i < column_.size(); i++) {

            std::cout << this->column_.getContent().at(i) << " , ";

        }

        std::cout << std::endl << "decompressed : " << decompress(column_.size() - 1) << " id : " << column_.size() - 1
                  << std::endl;


        return true;

    }


    template<typename T>

    template<typename InputIterator>

    bool DeltaCodingCompressedColumn<T>::insert(InputIterator, InputIterator) {


        return true;

    }


    template<class T>

    T &DeltaCodingCompressedColumn<T>::decompress(TID id) {

        std::vector <T> values = column_.getContent();

        T *result = new int;


        if (id == 0) {

            *result = values.at(id);

        } else {

            for (TID i = 0; i <= id; i++) {

                *result += values.at(i);

            }

        }



        //std::cout << std::endl << "decompress result for TID " << id << " : " << result << std::endl;



        return *result;

    }


    template<class T>

    const boost::any DeltaCodingCompressedColumn<T>::get(TID id) {

        std::cout << "return value from get : " << decompress(id) << std::endl;

        return decompress(id);

    }


    template<class T>

    void DeltaCodingCompressedColumn<T>::print() const throw() {


    }

    template<class T>

    size_t DeltaCodingCompressedColumn<T>::size() const throw() {

        return column_.size();

    }

    template<class T>

    const ColumnPtr DeltaCodingCompressedColumn<T>::copy() const {
        return ColumnPtr(new DeltaCodingCompressedColumn<T>(*this));

    }


    template<class T>

    bool DeltaCodingCompressedColumn<T>::update(TID id, const boost::any &newBoostValue) {
        T newValue = boost::any_cast<T>(newBoostValue);
        this->column_.print();
        if (id == 0) {

            //update old first value with new first value
            if (!this->column_.update(0, newValue)) {
                return false;
            }
            //adjust only first successor delta if present
            if (this->size() > 1){
                T decompressedSucc = boost::any_cast<T>(this->get(id + 1));
                T newSuccDelta = decompressedSucc - newValue;
                if (!this->column_.update(1, newSuccDelta)) {
                    return false;
                }
            }
        } else {
            std::cout << "newValue: " << newValue << std::endl;
            T oldDelta = this->column_[id];
            std::cout << "old delta: " << oldDelta << std::endl;
            T decompressedPreceding = boost::any_cast<T>(this->get(id - 1));
            std::cout << "decompressedPreceding: " << decompressedPreceding << std::endl;
            T newDelta = newValue - decompressedPreceding;
            std::cout << "newDelta: " << newDelta << std::endl;
            T updateDelta = newDelta - oldDelta;
            std::cout << "updateDelta: " << updateDelta << std::endl;
            //update old delta with new delta
            if (!this->column_.update(id,newDelta)) {
                return false;
            }
            //adjust succ delta
            //for (long unsigned int i = id + 1; i < this->column_.size(); i++) {
                if (!this->column_.update(id+1, this->column_[id+1] - updateDelta)) {
                    return false;
                }
            //}
        }
        this->column_.print();

        return true;

    }


    template<class T>

    bool DeltaCodingCompressedColumn<T>::update(PositionListPtr, const boost::any &) {

        return false;

    }


    template<class T>

    bool DeltaCodingCompressedColumn<T>::remove(TID) {

        return false;

    }


    template<class T>

    bool DeltaCodingCompressedColumn<T>::remove(PositionListPtr) {

        return false;

    }


    template<class T>

    bool DeltaCodingCompressedColumn<T>::clearContent() {

        return false;

    }


    template<class T>

    bool DeltaCodingCompressedColumn<T>::store(const std::string &) {

        return false;

    }

    template<class T>

    bool DeltaCodingCompressedColumn<T>::load(const std::string &) {

        return false;

    }


    template<class T>

    T &DeltaCodingCompressedColumn<T>::operator[](int index) {
        //T& t = index;
        return this->decompress(index);
        //std::cout << "key : " << key;
        //return t;
    }


    template<class T>

    unsigned int DeltaCodingCompressedColumn<T>::getSizeinBytes() const throw() {

        return 0; //return values_.capacity()*sizeof(T);

    }



/***************** End of Implementation Section ******************/







}; //end namespace CogaDB

