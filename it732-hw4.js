
use restaurants


//#Q1

db.getCollection("restaurants").count();
//#Q1

db.getCollection("restaurants").find({})
//#Q2

db.getCollection("restaurants").find({},{"restaurant_id":1,"name":1,"borough":1,"cuisine":1});

//#Q3

db.getCollection("restaurants").find({},{"restaurant_id":1,"name":1,"borough":1,"cuisine":1, "_id":0});

//#Q4 

db.getCollection("restaurants").find({},{"restaurant_id":1,"name":1,"borough":1,"address.zipcode":1, "_id":0});

//#Q5

db.getCollection("restaurants").find({borough:"Bronx"});

//#Q6

db.getCollection("restaurants").find({borough:"Bronx"}).limit(5);

//#Q7

db.getCollection("restaurants").find({borough:"Bronx"}).skip(5).limit(5);

//#Q8

db.getCollection("restaurants").find({"grades.score":{$gt: 85}});

//#Q9

db.getCollection("restaurants").find({"grades.score":{$gt: 80, $lt: 100}});

//#Q10
db.getCollection("restaurants").find({'address.coord.0': {$lt: -95.754168}});

//#Q11

db.getCollection("restaurants").find({$and: [{"cuisine":{$ne: "American "}}, {'address.coord.0': {$lt:  -65.754168}}, {"grades.score":{$gt: 70}}]});

//#Q12
db.getCollection("restaurants").find({"cuisine":{$ne: "American "}, 'address.coord.0': {$lt:  -65.754168}, "grades.score":{$gt: 70}});

//#Q13

db.getCollection("restaurants").find({"cuisine":{$ne: "American "}, "borough":{$ne: "Brooklyn"}, "grades.grade": "A"}).sort({"cuisine": -1});

//#Q14

db.getCollection("restaurants").find({name:/^Wil/}, {"restaurant_id":1,"name":1,"borough":1,"cuisine":1});

//#Q15

db.getCollection("restaurants").find({name:/ces$/}, {"restaurant_id":1,"name":1,"borough":1,"cuisine":1});

//#Q16

db.getCollection("restaurants").find({name:/Reg/}, {"restaurant_id":1,"name":1,"borough":1,"cuisine":1});

//#Q17

db.getCollection("restaurants").find({$and: [{$or: [ {cuisine:"American "}, {cuisine:"Chinese"}]}, {borough:"Bronx"}]});
 
//#Q18

db.getCollection("restaurants").find({$or:[{borough:"Brooklyn"}, {borough:"Bronx"}, {borough:"Queens"}, {borough:"Staten Island"}]}, {"restaurant_id":1,"name":1,"borough":1,"cuisine":1});

//#Q19

db.getCollection("restaurants").find({$nor:[{borough:"Brooklyn"}, {borough: "Bronx"}, {borough:"Queens"}, {borough:"Staten Island"}]}, {"restaurant_id":1,"name":1,"borough":1,"cuisine":1});

//#Q20

db.getCollection("restaurants").find({"grades.score":{$lt: 10}}, {"restaurant_id":1,"name":1,"borough":1,"cuisine":1});

//#Q21

db.getCollection("restaurants").find({$or: [{name: /^Wil/}, {"$and": [ {"cuisine": {$ne: "American "}}, {"cuisine": {$ne: "Chinese"}}]}]}, {"restaurant_id":1,"name":1,"borough":1,"cuisine":1});

//#Q22

db.getCollection("restaurants").find({"grades.score":11, "grades.grade":"A", "grades.date": ISODate("2014-08-11T00:00:00Z")}, {"restaurant_id":1,"name":1,"grades":1});

//#Q23

db.getCollection("restaurants").find({"grades.1.score":9, "grades.1.grade":"A", "grades.1.date": ISODate("2014-08-11T00:00:00Z")}, {"restaurant_id":1,"name":1,"grades":1});

//#Q24

db.getCollection("restaurants").find({"address.coord.1":{$gt: 42, $lte: 52}}, {"restaurant_id":1,"name":1,"address":1, "coord":1});

//#Q25

db.getCollection("restaurants").find().sort({"name": 1});
