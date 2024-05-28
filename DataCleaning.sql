-- Data Cleaning 

-- Steps to be done 
-- 1. Remove duplicates, if any 
-- 2. Standardize the data, solve issues with spellings or spaces
-- 3. Remove null and blank values, check if you can populate it or not 
-- 4. Remove any unnecessary columns or rows, only if its relevant

use world_layoffs;

-- We create a staging table as a copy of the original table so that our raw data can be preserved

CREATE TABLE layoffs_staging 
LIKE layoffs;

-- Populate the table 
INSERT layoffs_staging 
SELECT * FROM layoffs;

select * from layoffs_staging;

-- 1. Removing duplicates 

-- ROW_NUMBER() function to group by multiple fields.

with cte as (
select *, 
row_number() over(
partition by company, location, industry, total_laid_off, 
percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num from layoffs_staging2)
select * from cte where row_num >1;

-- We can't delete directly from cte so we transfer the data to another table.
-- Create another table layoffs_staging2 with one extra column called row_num.

CREATE TABLE layoffs_staging2 (
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` int,
row_num INT
);

-- Insert data into it 

INSERT INTO layoffs_staging2
SELECT *,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		layoffs_staging;
        
-- Delete data from the new table 

DELETE FROM layoffs_staging2
WHERE row_num >= 2;

-- Check for duplicates again

select * from layoffs_staging2 
where row_num =2;

-- 2. Standardization of Data

-- Used trim function to remove any unnecessary characters or any trailing and leading spaces in the company field. 

select trim(company) from layoffs_staging2;

-- Update the table with the new cleaned data 

update layoffs_staging2
set company = trim(company);

-- This revealed to us, problems in the industry column of the dataset.
-- We have similar names for one value, example crypto, crypto currencuy are the same thing, but
-- it is stored as distinct values, which can be combined and put as one value to denote the same thing.
-- There are null values in the field too, which is discussed in part 3.

select distinct(industry)
from layoffs_staging2
order by 1; 

-- Solving the composition of similar row values into one single categorical name. 
-- Only identified value is related to crypto.

select distinct industry from layoffs_staging2 
where industry like '%crypto%';

-- Most of the rows have crypto as the main value.
-- So, We update it to crypto.

select industry from layoffs_staging2 
where industry like '%crypto%'; 

-- Update to set the value, hard coded way
-- Re use the condition through which we came to our finding of the problem, which is where industry like '%crypto%'

update layoffs_staging2 
set industry = 'Crypto'
where industry like '%crypto%';

-- Moving on to checking another column data - Location
-- View the column values and take a quick scan through the rows

select distinct location from layoffs_staging2;

-- Repeat the same with country field

select distinct country from layoffs_staging2;

-- Same value for united states with a period at the end. This is supposedly a very common error.
-- Only 4 rows have this error, so a human entry error is likely the reason.

select country from layoffs_staging2 
where country like 'United States.';

-- We use trim function with trailing argument, to eliminate the period at the end.

select distinct country, trim(trailing '.' from country)
from layoffs_staging2;

-- Update the new cleaned data in the table using update, and reusing the condition about where we found the error, 
-- meaning identification of error values.

update layoffs_staging2 
set country = trim(trailing '.' from country)
where country like 'United States';

-- Moving ahead to check for data types of the fields
-- Changing the data type of date from text to date using str_to_date() function.
-- The format of the second argument of the str_to_date() function has to be strictly followed.
-- Arrangement of '%m/%d/%Y' has to be strictly of this format.(Play yourself by changing the 
-- case(upper or lower) of the letters, you will see)

select `date`, 
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2;

-- Update in the table 

update layoffs_staging2
set date = str_to_date(`date`, '%m/%d/%Y');
 
 select `date` from layoffs_staging2;
 
 alter table layoffs_staging2 
 modify column `date` date;
 
 -- 3. Remove Null and blank values 

-- We have many null values here

select total_laid_off from layoffs_staging2;
 
 -- Check the dependent field on this which is the percentage of laid off to check if we have null 
 -- values for both the fields. Check how many row numbers have null values for both the fields.
 
SELECT 
    *
FROM
    layoffs_staging2
WHERE
    total_laid_off IS NULL
        AND percentage_laid_off IS NULL;

-- Re-iterating to industry field, which had null values 

select * from layoffs_staging2
where industry is null or 
industry = '';

-- Let's look at other row data for companies mentioned 

select * from layoffs_staging2
where company like '%Airbnb%';

-- We have this problem for other companies also.
-- So, We use self join to address the issue of updating the null industry values to its respective populated
-- entry from the other row.

-- Let's look for all the null and blank industry values in the table using self join

select t1.industry, t2.industry from  layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

-- We have blank values alongwith null values for both the fields which are under comparison.
-- So, the wise step is to always set the blank industry values to null values first, before moving forward
-- with removing the null values.

update layoffs_staging2
set industry = NULL 
where industry = '';

-- Now we only have the null values against its populated counterpart
-- Let's update the changes to the table 

Update layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
set t1.industry = t2.industry 
where t1.industry is null
and t2.industry is not null;
 
 -- Look for the null values in the table again to double check
 
 select * from layoffs_staging2
 where industry is null;
 
-- Moving forward to other fields

 -- We have 361 rows of no data values in both 'total_laid_off' and 'percentage_laid_off' columns
 -- This table is concerned with the layoff data of companies
 -- Since we have no data in both the columns, we can safely assume that there must not be any layoffs to be reported
 -- We come to this assumption only because there is a lack of data, as far as we are concerned
 -- So, We can put these data values under 'unnecessary data' category and consider getting rid of it
 
SELECT 
    *
FROM
    layoffs_staging2
WHERE
    total_laid_off IS NULL
        AND percentage_laid_off IS NULL;

-- Deleting irrelevant data 

delete from layoffs_staging2
where   total_laid_off IS NULL
        AND percentage_laid_off IS NULL;

-- 4. Removing Unnecessary columns or rows

-- Removing the row_num column from the table 

alter table layoffs_staging2 
drop column row_num;

select * from layoffs_staging2 
limit 5;
 
