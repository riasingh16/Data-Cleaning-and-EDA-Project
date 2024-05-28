-- Exploratory Data Analysis (with layoffs)

select * from layoffs_staging2;

-- Company against total laid off arranged with highest laid off at first 
-- We can limit to 5 or 10 companies with the most number of laid off and use in chart in POWER BI 

select company, sum(total_laid_off)
from layoffs_staging2
group by 1
order by 2 desc; 

-- Maximum number of employees laid off from a company at once was Google 

 select max(total_laid_off), company
 from layoffs_staging2 
 group by company
 order by 1 desc
 limit 1;
 
 -- Maximum percentage of people laid off from a company 
 -- Percentage of 1 represents, 100% of its employees were laid off
 
 select max(percentage_laid_off)
 from layoffs_staging2;
 
 -- 116 companies laid off all of it's staff

select count(*)
from layoffs_staging2 where percentage_laid_off = 1;

-- Date range of this data to find out between what years these layoffs have taken place
-- It's between March 2020 and March 2023
-- Pandemic behaviour might show in data 

select min(`date`), max(`date`)
from layoffs_staging2;

-- Industy against total number of employees laid off 
-- Consumer and Retail have the highest number 
-- Use limit to see top 5 or 10 industry which laid off maximum number of employees

select industry,sum(total_laid_off) as laid_off
from layoffs_staging2
group by 1 
order by 2 desc;

-- Country against maximum employees laid off
-- United States followed by India and Netherlands laid off the most number of employees

select country, sum(total_laid_off)
from layoffs_staging2 
group by 1
order by 2 desc;

-- Which year faced the highest layoffs
-- 2023 only has data for three months till now but the employees laid off is close to the maximum limit
-- which could mean that 2023 will see the maximum lay offs, compared to past three years

select year(`date`), sum(total_laid_off)
from layoffs_staging2
group by 1
order by 2 desc;

-- Time series analysis of data by month 
-- substring function used, to fetch the year and month part of date column against sum of staff laid off 

select substring(`date`, 1, 7) as `month`,
sum(total_laid_off)
from layoffs_staging2
where substring(`date`, 1, 7) is not null
group by 1
order by 1;

-- Cumulative sum of total laid off by month using CTE

-- Used substring function to fetch from first character to the seventh character of date
-- Rolling total is calculated by summing the total laid off after each row to its previous entry 
-- we also found the total number of employees laid off within the past 4 years by cumulative sum. 

with cumulative as (
select substring(`date`, 1, 7) as `month`,
sum(total_laid_off) as laid_off
from layoffs_staging2
where substring(`date`, 1, 7) is not null
group by 1
order by 1)
select `month`, laid_off
,sum(laid_off) over(order by `month`) as 'rolling total' 
from cumulative;

-- Yearly top 5 companies with highest lay offs 
-- Yearly comparison of companies who laid off the highest employees in sub CTE

-- Used dense rank function for grouping the data by years arranged against 
-- total number of employees laid off against that year from highest order first.
-- Used concat function to bring the company name and the number of employees laid off under one column

with company_year as (select company, year(`date`) as years, sum(total_laid_off) as total_laid_off
from layoffs_staging2 
group by 1, 2), company_year_rank as(
select *, 
dense_rank() over(partition by years order by total_laid_off desc) as ranking
from company_year
where years is not null)
select years, concat(company, ' (', total_laid_off,') ') as company_data
from company_year_rank 
where ranking <=5
group by 1, 2;
 


