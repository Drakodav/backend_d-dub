# deletes from the gtfsRApi from a given month

# """
# delete from "gtfsRApi_gtfsrapi"
# where id in (
# 	select id from "gtfsRApi_gtfsrapi"
# 	where timestamp < '2021-02-01'
# )
# ;
# """