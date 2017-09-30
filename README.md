## File descriptions
* AllQMapper.java - the mapper that reads in the data and produces key value pairs based on the question and the required output for that question
* AllQCombiner.java - aids in speeding up the adding of the values for each question
* AllQReducer.java - outputs the final values for questions Q1-Q6 and intermediate values for Q7 and Q8
* Q7MapperPart2.java & Q7ReducerPart2.java - sorts the average number of rooms per house and then provides the 95th percentile.
* Q8MapperPart2.java & Q8ReducerPart2.java - sorts all the percentages of elderly by taking advantage of the inhouse sorting of mapreduce and outputs the state with the most elderly population.
* AllQRunner.java - Runs the three jobs in the exact order (also contains code that could do jobs 2 & 3 if only one map reduce job was wanted and using outside java programs was allowed)
* ValueWritable.java - A custom writable that contains up to four values to be used by all the questions

The program outputs in the following format:
```
<Question_number>/<Question_number>-r-00000
Q7Result/part-r-00000
Q8Result/part-r-00000
```

Multiple outputs were chosen to provide better analysis of the different factors as well as easier integration into the google maps frameworks (still needs to be done)

## Required questions
1. On a per-state basis provide a breakdown of the percentage of residences that were rented vs.
owned. 
2. On a per-state basis what percentage of the population never married? Report this for both
males and females. Note: The US Census data tracks this information for persons with ages 15
years and over.
3. On a per-state basis, analyze the age distribution based on gender.
    1. Percentage of people below 18 years (inclusive) old.
    2. Percentage of people between 19 (inclusive) and 29 (inclusive) years old.
    3. Percentage of people between 30 (inclusive) and 39 (inclusive) years old.
4. On a per-state basis, analyze the distribution of rural households vs. urban households.
5. On a per-state basis, what is the median value of the house that occupied by owners?
6. On a per-state basis, what is median rent paid by households?
7. What is the 95th percentile of the average number of rooms per house across all states?
8. Which state has the highest percentage of elderly people (age > 85) in their population?
