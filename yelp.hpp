#include <memory>
#include <odb/core.hxx>
#include <odb/lazy-ptr.hxx>
#include <set>
#include <string>

#pragma db view
class StarCount{
public:
        int stars;
        int count;
};

#pragma db view query("select top 1 text, last_elapsed_time from sys.dm_exec_query_stats cross apply sys.dm_exec_sql_text(sql_handle) order by last_execution_time desc")
class LastQueryTime{
public:
        std::string text;
        long elapsed_time;
};

// ---------------------------------------------
// No need to change anything above this line
// ---------------------------------------------

class User;
class Business;
class Hours;
class Review;

#pragma db object
class User{
public:
        #pragma db id
        std::string id;
        std::string name;

        #pragma db inverse(user_id)
        std::vector<std::shared_ptr<Review>> reviews_;
};

#pragma db object
class Hours{
public:
        #pragma db id
        int id;
        std::string hours;

        #pragma db not_null
        odb::lazy_shared_ptr<Business> business_id;
};

#pragma db object
class Review{
public:
        #pragma db id
        std::string id;
        std::string business_id;
        int stars;

        #pragma db not_null
        odb::lazy_shared_ptr<User> user_id;
};

#pragma db object
class Business{
public:
        #pragma db id
        std::string id;

        float latitude;
        float longitude;
        float stars;

        #pragma db inverse(business_id)
        std::vector<std::shared_ptr<Hours>> hours_;

};
