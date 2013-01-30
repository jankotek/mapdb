<script src='js/twitter.js'></script>
<table>
<tr>
    <td width='70%'>
MapDB provides concurrent TreeMap and HashMap backed by disk storage or off-heap memory.
It is a fast, scalable and easy to use embedded Java database engine. It is tiny (160KB jar),
yet packed with features such as transactions, space efficient serialization, instance cache
and transparent compression/encryption. It also has outstanding performance rivaled only by
native embedded db engines.
<div class='aright'><a href='features.html'>Learn what MapDB can do</a></div>
    </td>

    <td data-limit='15' id='buzz' rowspan='3' valign='top'>
        <h4>What people are saying</h4>
        <ul></ul>
        <div class='aright'><a href='http://twitter.com/search?q=mapdb&src=typd'>More tweets...</a></div>
    </td>
</tr>
<tr>
    <td>
        <h2>News</h2>
        <p>30/1/2013 MapDB <a href="http://youtu.be/FdZmyEHcWLI">overview video</a></p>

        <div class='aright'>
         Follow news:
         <a href='https://groups.google.com/group/mapdb-news/feed/rss_v2_0_msgs.xml?num=50'>RSS</a> |
         <a href='https://groups.google.com/forum/?fromgroups#!forum/mapdb-news'>Mail-List</a> |
         <a href='http://twitter.com/MapDBnews'>Twitter</a>
        </div>
    </td>
</tr>

<tr>
    <td>
<h2>Hello world</h2>
<pre>
import org.mapdb.*;

//Configure and open database using builder pattern.
DB db = DBMaker
    .newFileDB(new File("testdb"))
    .closeOnJvmShutdown()
    .make();

//create new collection (or open existing)
ConcurrentNavigableMap<Integer,String> map = db.getTreeMap("collectionName");
map.put(1,"one");
map.put(2,"two");

//persist changes into disk, there is also rollback() method
db.commit();

db.close();
</pre>
<div class='aright'>
<a href='doc/10-intro.html'>Introduction</a> |
<a href='https://github.com/jankotek/MapDB/tree/master/src/test/java/examples'>More examples</a>
</div>
    </td>
</tr>
<tr>
</table>


