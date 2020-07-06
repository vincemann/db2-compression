

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
        return this->column_.insert(boost::any_cast<T>(value));
    }


    template<class T>

    bool DeltaCodingCompressedColumn<T>::insert(const T &value) {

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

        std::cout << std::endl << "decompressed : " << decompress(column_.size() - 1) << " id : " << column_.size() - 1
                  << std::endl;


        return true;

    }


    template<typename T>

    template<typename InputIterator>

    bool DeltaCodingCompressedColumn<T>::insert(InputIterator start, InputIterator end) {
        bool inserted = true;
        for (auto it = start; it != end; ++it) {
            inserted = insert(*it);
        }
        return inserted;

    }


    template<class T>

    T &DeltaCodingCompressedColumn<T>::decompress(TID id) {

        std::vector <T> values = column_.getContent();

        T *result = new int;
        *result = 0;


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
        return decompress(id);
    }


    template<class T>

    void DeltaCodingCompressedColumn<T>::print() const throw() {
        return column_.print();
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
        //this->column_.print();
        if (id == 0) {

            //update old first value with new first value
            if (!this->column_.update(0, newValue)) {
                return false;
            }
            //adjust successor delta if present
            if (this->size() > 1) {
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
            if (!this->column_.update(id, newDelta)) {
                return false;
            }
            //adjust succ delta
            if (!this->column_.update(id + 1, this->column_[id + 1] - updateDelta)) {
                return false;
            }

        }
        //this->column_.print();

        return true;

    }


    template<class T>

    bool DeltaCodingCompressedColumn<T>::update(PositionListPtr ptr, const boost::any &value) {
        if (value.empty() || typeid(T) != value.type()) {
            return false;
        }
        for (auto it = ptr->begin(); it != ptr->end(); ++it) {
            this->update(*it, value);
        }
        return true;

    }


    template<class T>

    bool DeltaCodingCompressedColumn<T>::remove(TID id) {
        if(id==0){
            T decompressedSucc = boost::any_cast<T>(this->get(id + 1));
            this->column_.getContent().erase(this->column_.getContent().begin()+0);
            if(!this->column_.update(0,decompressedSucc)){
                return false;
            }
        }else {
            T decompressedPreceding = boost::any_cast<T>(this->get(id - 1));
            T decompressedSucc = boost::any_cast<T>(this->get(id + 1));
            T newSuccDelta =  decompressedSucc - decompressedPreceding;
            this->column_.getContent().erase(this->column_.getContent().begin() + id);
            if(this->column_.update(id, newSuccDelta)){
                return false;
            }
        }
        return true;
    }


    template<class T>

    bool DeltaCodingCompressedColumn<T>::remove(PositionListPtr ptr) {
        if (!ptr || ptr->empty()) {
            return false;
        }

        for (PositionList::reverse_iterator rit = ptr->rbegin(); rit != ptr->rend(); ++rit) {
            this->remove(*rit);
        }

        return true;
    }


    template<class T>

    bool DeltaCodingCompressedColumn<T>::clearContent() {
        return this->column_.clearContent();
    }


    template<class T>

    bool DeltaCodingCompressedColumn<T>::store(const std::string &path_) {
        std::string path(path_);
        path += "/";
        path += this->name_+"-meta";
        //std::cout << "Writing Column " << this->getName() << " to File " << path << std::endl;
        std::ofstream outfile (path.c_str(),std::ios_base::binary | std::ios_base::out);
        boost::archive::binary_oarchive oa(outfile);

        //std::cout << "storing last_value_: " << last_value_ << std::endl;
        //std::cout << "before store: " << std::endl;
        //this->column_.print();
        oa << last_value_;

        outfile.flush();
        outfile.close();
        return this->column_.store(path_);
    }

    template<class T>

    bool DeltaCodingCompressedColumn<T>::load(const std::string &path_) {
        std::string path(path_);
        //std::cout << "Loading column '" << this->name_ << "' from path '" << path << "'..." << std::endl;
        //string path("data/");
        path += "/";
        path += this->name_+"-meta";

        //std::cout << "Opening File '" << path << "'..." << std::endl;
        std::ifstream infile (path.c_str(),std::ios_base::binary | std::ios_base::in);
        boost::archive::binary_iarchive ia(infile);

        ia >> last_value_;
        std::cout << "loaded last_value_: " << last_value_ << std::endl;


        infile.close();


        bool loaded  = this->column_.load(path_);
        //std::cout << "after load: " << std::endl;
        //this->column_.print();
        return loaded;
    }


    template<class T>

    T &DeltaCodingCompressedColumn<T>::operator[](int index) {
        return this->decompress(index);
    }


    template<class T>

    unsigned int DeltaCodingCompressedColumn<T>::getSizeinBytes() const throw() {
        return this->column_.getSizeinBytes();
    }



/***************** End of Implementation Section ******************/







}; //end namespace CogaDB

