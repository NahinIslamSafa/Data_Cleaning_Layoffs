use world_layoffs;
select * from layoffs;

CREATE TABLE layoffs_staging 
LIKE layoffs;

INSERT layoffs_staging 
select * from layoffs;

select * from layoffs_staging;

SELECT * ,
ROW_NUMBER() OVER(PARTITION BY company, industry, total_laid_off,`date`) AS row_num 
from layoffs_staging;

WITH duplicate_cte AS 
(
SELECT * ,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off,
percentage_laid_off,`date`, stage, country, funds_raised_millions ) AS row_num 
from layoffs_staging
)
SELECT * 
from duplicate_cte
WHERE row_num > 1;

SELECT *
FROM layoffs_staging
WHERE company = 'Casper';

CREATE TABLE `world_layoffs`.`layoffs_staging2`(
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2 
SELECT * ,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off,
percentage_laid_off,`date`, stage, country, funds_raised_millions ) AS row_num 
from layoffs_staging ;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

DELETE 
FROM layoffs_staging2
WHERE row_num > 1;

select company,trim(company)
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

select distinct(industry)
from layoffs_staging2
order by 1;

select * 
from layoffs_staging2 
where industry like 'Crypto%';

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

select distinct country
from layoffs_staging2;

select distinct country,TRIM(TRAILING '.' FROM country)
from layoffs_staging2 
order by 1;

update layoffs_staging2
set country = TRIM(TRAILING '.' FROM country);
-- where country like 'United States%';

select`date`, STR_TO_DATE(`date`, '%m/%d/%Y')
from layoffs_staging2 ;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

select`date`
from layoffs_staging2 ;

alter table layoffs_staging2
modify `date` date ;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

update layoffs_staging2
set industry = null
where industry = '';

select *
from layoffs_staging2 t1
join layoffs_staging2 t2
   on t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null ;

select t1.industry,t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
   on t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null ;

update layoffs_staging2 t1
join layoffs_staging2 t2
   on t1.company = t2.company
   set t1.industry = t2.industry
where t1.industry is null 
and t2.industry is not null ;

select *
from layoffs_staging2 ;

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null ;

delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null ;

select *
from layoffs_staging2;

alter table layoffs_staging2
drop column row_num;


