#Q1

a <- 32*(33/8)

#Q2 Result = 56.65

a <- a/2.33
a

#Q3 

q3 <- -8.2*10-13
q3

#Q4

a*q3

#Q5

q5 <- seq(5,-11, by= -0.3)
q5

#q6

q5 <- rev(q5)
q5

#Q7

rep((rep(c(-1,3,-5,7,-9), times = 2)), each=10) 
x <- sort(rep((rep(c(-1,3,-5,7,-9), times = 2)), each=10), decreasing = TRUE) 
x

#Q8

q8 <- c(6:12, rep(5.3, times =3), -3, seq(102, length(x), by = -0.25))
q8

#Q9

length(q8)

#Q10

q10 <- c(seq(3,6, by= 0.75), rep(c(2,-5.1,-33), times=2), 7*42+2 )
q10

#Q11

f <- head(q10, n=1)
t <- tail(q10, n=1)
q11 <- c(f,t)
q11

#Q12

remove1 <- c(f,t)
q12 <- q10[!q10 %in% remove1]
q12

#Q13

q13 <- c(q11, q12)
q13 <- c(q13[-2],q13[2])
q13

#Q14

q14 <- sort(q10)
q14

#Q15

q15 <- q10[length(q10):1]
q15
q15_sort = sort(q10, decreasing = TRUE)
q15_sort

#Q16

q16 <- c(rep(q12[3], times=3),rep(q12[6], times=4),rep(tail(q12, n=1), times=1))
q16        

#Q17

q17 <- q14
q17[1] <- 99 
q17[5:7]<-98:96
q17[length(q17)] <- 95
  
#Q18
# yes we can have a dataframe with zero column and zero rows  
q18 <- data.frame()  
q18

#Q19

vec1 <- c(2,5,8,12,16)
vec1

#Q20

vec2 <- 5:9
vec2

#Q21

vec1-vec2

#Q22

q22 <- seq(2, length.out = 100, by=3)
q22

#Q23

q22[c(5,10,15,20)]

#Q24

q22[10:30]

#Q25

library(dplyr)
library(nycflights13)
filter(flights, dep_delay >= 60, (dep_delay - arr_delay) > 30)
