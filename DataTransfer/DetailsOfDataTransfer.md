# Why the code was made this way

As you can see there are many repeating functions that could have been made into one function with a for loop. Unfortunately the whole case study was to be done on a single local comupter. The little machine could not handle rapid spark Dataframe creation, there just not enough RAM. So I divided each of the Kafka topic producing and Spark Dataframe creation into seperate functions. With that I was able to give time between all of the RAM use. 
