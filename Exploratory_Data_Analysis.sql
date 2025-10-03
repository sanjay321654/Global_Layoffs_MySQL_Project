use world_layoffs;

select *
from layoffs_staging2;

-- Finding Companies with large amount of layoffs in a single day. 
select company, total_laid_off
	from layoffs_staging2
	where total_laid_off = (
		select max(total_laid_off)
		from layoffs_staging2 
      );
      
-- Finding Companies with 100 percentage  of layoffs (1 represents 100 %).
select company, total_laid_off
from layoffs_staging2
where percentage_laid_off = 1;
    
/* Combining these two, using CTEs for checking if any company has highest total laid off (Like Google)
 and also laid out its 100 percent employees. That means they going to shutdown their company. */
with max_total_cte as
(	select company, total_laid_off
	from layoffs_staging2
	where total_laid_off = (
		select max(total_laid_off)
		from layoffs_staging2
	)
 ),  

max_percent_cte as
(	select company, percentage_laid_off
	from layoffs_staging2
	where percentage_laid_off = 1
)

select c1.company, c1.total_laid_off, c2.percentage_laid_off
from max_total_cte as c1
join max_percent_cte as c2
	on c1.company = c2.company
;


-- Finding total number laid off in different days for same company.
select company, sum(total_laid_off) as total
from layoffs_staging2
group by company
order by total desc ;

-- Finding maximum number of laid off on the basis of country.
select country, sum(total_laid_off) as total
from layoffs_staging2
group by country
order by total desc ;

-- Finding maximum number of laid off in a year.
select year(`date`) as yr, sum(total_laid_off) as total
from layoffs_staging2
group by yr
order by total desc ;

-- Finding total number of laid off in all the given years by using the rolling total on month wise .
select substring(`date`, 1, 7) as `month`, sum(total_laid_off) as total
from layoffs_staging2
where substring(`date`, 1, 7) is not null
group by `month`
order by 1 asc;

with rolling_total as
( select substring(`date`, 1, 7) as `month`, sum(total_laid_off) as total
from layoffs_staging2
where substring(`date`, 1, 7) is not null
group by `month`
order by 1 asc
)
select `month`, total, sum(total)
over(order by `month`) as r_total
from rolling_total;

-- Finding individual Company laid off record.
select company, year(`date`), sum(total_laid_off) as total
from layoffs_staging2
group by company, year(`date`)
order by total desc ;

with Company_Year as
(	select company, year(`date`) as years, sum(total_laid_off) as total
	from layoffs_staging2
	group by company, year(`date`)
 ),
Company_Rank as
(	select *, 
	dense_rank() over(partition by years order by total desc) as ranking
	from Company_Year
    where years is not null
)
-- Picking top 5 companies for each year.

select *
from Company_Rank
where ranking <= 5;
















