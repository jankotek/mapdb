Benchmarks
========
<!--

<script src="js/jquery-1.8.3.min.js" />
<script src="js/excanvas.js" />
<script src="js/js-class.js" />
<script src="js/bluff.js" />
<script src="js/benchmarks-data.js" />
<script src="js/benchmarks.js" />
<span id="benchmark" />

<table id="benchmarks-table">
<tr>
<th></th>
<th>Read</th>
<th>Write</th>
</tr>
</table>

-->
 ConcurrentHashMap

    Insert       2,940 ms
    Iterate      257 ms 
    RndRead      828 ms
    RndUpdate    4,245 ms

MapDB TreeMap

    Insert       9,538 ms
    Iterate      853 ms
    RndRead      3,453 ms
    RndUpdate    13,668 ms

MapDB HashMap
   
    Insert       11,198 ms
    Iterate      1,393 ms
    RndRead      3,706 ms
    RndUpdate    13,451 ms

MapDB HashMap - Heap Store 

    Insert       3,949 ms
    Iterate      2,075 ms
    RndRead      9,744 ms
    RndUpdate    23,081 ms
