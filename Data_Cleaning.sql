
/* Creating a table called layoffs_staging to store all the existing data 
in the layoffs table. Because it is not the good practice to work with the raw data. */

create table layoffs_staging
like layoffs;

-- Inserting the data from layoffs table to layoffs_staging table.
insert layoffs_staging
	select *
    from layoffs;
    
select * from layoffs_staging;

-- 1. Remove Duplicates
select *, row_number() over(
 partition by company, location, industry, total_laid_off,
 percentage_laid_off, `date`, stage, funds_raised_millions) as row_num
from layoffs_staging;

with duplicate_cte as
( select *, row_number() over(
 partition by company, location, industry, total_laid_off,
 percentage_laid_off, `date`, stage, funds_raised_millions) as row_num
from layoffs_staging

)
select *
from duplicate_cte 
where row_num > 1;

/*Now, it was not allowed to delete using CTEs. So we create a another table with the result 
   of the duplicate_cte then perform the delete operation. */
   CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` bigint DEFAULT NULL,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into layoffs_staging2
	select *, row_number() over(
	 partition by company, location, industry, total_laid_off,
	 percentage_laid_off, `date`, stage, funds_raised_millions) as row_num
	from layoffs_staging;

delete
from layoffs_staging2
where row_num >1;



-- 2. Standardize the data

select distinct country
from layoffs_staging2
order by 1 ;

select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1;

/* To revove the . from the end of the united states in a particular row because 
if we used distinct it will shows as a distinct country rather than US*/

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States';

select distinct country
from layoffs_staging2;

-- Updating the already existing date values from (text foramt) to actual (date format).
select `date`
from layoffs_staging2 ;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

/*Now, changing the datatype of date from text to date (datatype)
 so that if new values are inserted then it will be stored as a date format. */
 
 alter table layoffs_staging2
 modify column `date` date;

-- 3. Null Values or Blank Values
select * 
from layoffs_staging2
where industry is null 
or industry = '' ;

update layoffs_staging2
set industry = null
where industry = '' ;


select *
from layoffs_staging2
where company like 'Airbnb';

/* Here Airbnb has 2 rows one has a value in industry column and another one has not,
so now we are going to put the same value that the another one had to the null values industry column.
 for all companies its the same thing.*/

select *
from layoffs_staging2 as s1
join layoffs_staging2 as s2
	on s1.company = s2.company
where s1.industry is null
and s2.industry is not null;


update layoffs_staging2 as s1
join layoffs_staging2 as s2
	on s1.company = s2.company
    set s1.industry = s2.industry
where s1.industry is null
and s2.industry is not null;


-- 4. Remove any columns

select * 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

-- We don't need row_num anymore.
alter table layoffs_staging2
drop column row_num;
