#include "Implementation.hpp"
#include <iostream>

using namespace std;
//////////////////// Nested Loop Joins ////////////////////

std::vector<std::string> getQualifyingBusinessesIDsVector(Businesses const& b, float latMin,
                                                          float latMax, float longMin,
                                                          float longMax) {
        // This function needs to find all businesses that have within the
        // specified latitude/longitude range and store their ids in the result vector

        std::vector<std::string> result;

        for (size_t i = 0; i < b.ids.size(); i++) {
                if (b.latitudes[i] <= latMax && b.latitudes[i] >= latMin &&
                    b.longitudes[i] <= longMax && b.longitudes[i] >= longMin) {
                        result.push_back(b.ids[i]);
                }

        }

        return result;


//      std::cout << "function getQualifyingBusinessesIDsVector not implemented" << std::endl;
//      throw std::logic_error("unimplemented");
}

std::vector<unsigned long>
performNestedLoopJoinAndAggregation(Reviews const& r, std::vector<std::string> const& qualifyingBusinessesIDs) {
        // The second parameter of this function is the vector of qualifying
        // business ids you have created in the first function

        // This function needs to find all reviews that have business_ids in
        // the qualifyingBusinessesIDs vector and build a histogram over their stars
        // The return value is that histogram
        std::vector<unsigned long> histogram(6);

        for (size_t business = 0; business < qualifyingBusinessesIDs.size(); business++) {

                for (size_t review = 0; review < r.business_ids.size(); review++) {
                        if (qualifyingBusinessesIDs[business] == r.business_ids[review]){
                                histogram[r.stars[review]]++;
                        }
                }

        }

        return histogram;

//      std::cout << "function performNestedLoopJoinAndAggregation not implemented" << std::endl;
//      throw std::logic_error("unimplemented");
}

//////////////////// Hash Join ////////////////////

std::unordered_set<std::string> getQualifyingBusinessesIDs(Businesses const& b, float latMin,
                                                           float latMax, float longMin,                                                                                                                                                                                                           float longMax) {
        // This function needs to find all businesses that have within the
        // specified latitude/longitude range and store their ids in the result set

        std::unordered_set<std::string> result;

        for (size_t i = 0; i < b.ids.size(); i++) {

                if (b.latitudes[i] <= latMax && b.latitudes[i] >= latMin &&
                    b.longitudes[i] <= longMax && b.longitudes[i] >= longMin) {
                       result.insert(b.ids[i]);
                }

        }

        return result;

//      std::cout << "function getQualifyingBusinessesIDs not implemented" << std::endl;
//      throw std::logic_error("unimplemented");
}

std::vector<unsigned long>
aggregateStarsOfQualifyingBusinesses(Reviews const& r, std::unordered_set<std::string> const& qualifyingBusinesses) {
        // The second parameter of this function is the set of qualifying
        // business ids you have created in the first function

        // This function needs to find all reviews that have business_ids in
        // the qualifyingBusinessesIDs vector and build a histogram over their stars
        // The return value is that histogram

        std::vector<unsigned long> histogram(6);

        for (size_t i = 0; i < r.business_ids.size(); i++) {
                if (qualifyingBusinesses.count(r.business_ids[i])){
                        histogram[r.stars[i]]++;
                }
        }

        return histogram;

//      std::cout << "function aggregateStarsOfQualifyingBusinesses not implemented" << std::endl;
//      throw std::logic_error("unimplemented");
}
                                        
                                                                                                                                                                                                                     1,1           Top

