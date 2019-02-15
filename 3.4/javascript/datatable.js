/*
<!--script src="https://code.jquery.com/jquery-1.12.4.js"></script-->
<!--script src="https://cdn.datatables.net/1.10.13/js/jquery.dataTables.min.js"></script-->
<script>
</script>
*/
$(document).ready(function() {
    $('#table-all table').DataTable({"aLengthMenu": [[20,40, 80, -1], [20,40, 80, "All"]],"pageLength": 20});
} );
