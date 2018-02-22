

SELECT id, Tags 
from posts 
where CreationDate BETWEEN '01/01/2013' AND '12/31/2013' 
	AND AnswerCount >1 AND Tags <> '' 
ORDER BY NEWID()