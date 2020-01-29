exports.Criteria = function(id,title,state)
   {
    var obj = 
    {
        criteria_id             :    id,
        criteria_title          :    title,
        criteria_state          :    state,
        criteria_detail_set     :    []
    }
    return obj;
   }
exports.Criteria_detail = function(id, table, filed, condition, value,criteria_id)
{
    var obj =
    {
        criteria_detail_id          :   id,
        criteria_detail_table       :   table,
        criteria_detail_field       :   filed,
        criteria_detail_condition   :   condition, 
        criteria_detail_value       :   value,
        criteria_id                 :   criteria_id
    }
    return obj;
}

exports.addCriteriaDetail = function(criteriaSet, rows)
{
    var detail = Criteria_detail(rows.criteria_detail_id,rows.criteria_detail_table,criteria_detail_field,criteria_detail_condition,criteria_detail_value,criteria_detail_state,criteria_id);

    for(var i=0;i<criteriaSet.length;i++){
        if(criteriaSet[i].criteria_id==detail.criteria_id){
            criteriaSet[i].criteria_detail_set.push(detail);
        }
    }
    return criteriaSet;

}
