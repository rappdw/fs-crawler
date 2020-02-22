# Implementation Notes

## Lazy Resolution of Parent/Child Relationships

Unfortunately, the persons api doesn't return facts on the
parent child relationships. For the purpose of Red Black Graph 
exploration, we are only interested in BiologicalParent relationship.

The initial implementation would resolve all Parent/Child relationships to
determine the relationship type. This is expensive in the number of API calls.

The current implementation provides an "UntypedParent" relationship type (with the
assumption that represents BiologicalParent). There are two modes of resolution,
laxy (default) which only resolves Parent/Child relationships when there are more
than two parents for a given child. The second mode does no resolution at all.

The following are some sample timings

   
| Implementation  | API & Timing Detail                                                             |
|-----------------|---------------------------------------------------------------------------------|
| Original        | Downloaded 998 individuals, 3472 frontier,,141 seconds with 3273 HTTP requests. |
| Lazy Resolution | Downloaded 998 individuals, 3474 frontier,,34 seconds with 527 HTTP requests.   |
| No Resolution   | Downloaded 998 individuals, 3465 frontier,,10 seconds with 9 HTTP requests.     |

Note: I'm not sure I can explain the differences in frontier count