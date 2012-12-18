/**
 * Bluff - beautiful graphs in JavaScript
 * ======================================
 * 
 * Get the latest version and docs at http://bluff.jcoglan.com
 * Based on Gruff by Geoffrey Grosenbach: http://github.com/topfunky/gruff
 * 
 * Copyright (C) 2008-2010 James Coglan
 * 
 * Released under the MIT license and the GPL v2.
 * http://www.opensource.org/licenses/mit-license.php
 * http://www.gnu.org/licenses/gpl-2.0.txt
 **/

Bluff = {
  // This is the version of Bluff you are using.
  VERSION: '0.3.6',
  
  array: function(list) {
    if (list.length === undefined) return [list];
    var ary = [], i = list.length;
    while (i--) ary[i] = list[i];
    return ary;
  },
  
  array_new: function(length, filler) {
    var ary = [];
    while (length--) ary.push(filler);
    return ary;
  },
  
  each: function(list, block, context) {
    for (var i = 0, n = list.length; i < n; i++) {
      block.call(context || null, list[i], i);
    }
  },
  
  index: function(list, needle) {
    for (var i = 0, n = list.length; i < n; i++) {
      if (list[i] === needle) return i;
    }
    return -1;
  },
  
  keys: function(object) {
    var ary = [], key;
    for (key in object) ary.push(key);
    return ary;
  },
  
  map: function(list, block, context) {
    var results = [];
    this.each(list, function(item) {
      results.push(block.call(context || null, item));
    });
    return results;
  },
  
  reverse_each: function(list, block, context) {
    var i = list.length;
    while (i--) block.call(context || null, list[i], i);
  },
  
  sum: function(list) {
    var sum = 0, i = list.length;
    while (i--) sum += list[i];
    return sum;
  },
  
  Mini: {}
};

Bluff.Base = new JS.Class({
  extend: {
    // Draw extra lines showing where the margins and text centers are
    DEBUG: false,
    
    // Used for navigating the array of data to plot
    DATA_LABEL_INDEX: 0,
    DATA_VALUES_INDEX: 1,
    DATA_COLOR_INDEX: 2,
    
    // Space around text elements. Mostly used for vertical spacing
    LEGEND_MARGIN: 20,
    TITLE_MARGIN: 20,
    LABEL_MARGIN: 10,
    DEFAULT_MARGIN: 20,
    
    DEFAULT_TARGET_WIDTH:  800,
    
    THOUSAND_SEPARATOR: ','
  },
  
  // Blank space above the graph
  top_margin: null,
  
  // Blank space below the graph
  bottom_margin: null,
  
  // Blank space to the right of the graph
  right_margin: null,
  
  // Blank space to the left of the graph
  left_margin: null,
  
  // Blank space below the title
  title_margin: null,
  
  // Blank space below the legend
  legend_margin: null,
  
  // A hash of names for the individual columns, where the key is the array
  // index for the column this label represents.
  //
  // Not all columns need to be named.
  //
  // Example: {0: 2005, 3: 2006, 5: 2007, 7: 2008}
  labels: null,
  
  // Used internally for spacing.
  //
  // By default, labels are centered over the point they represent.
  center_labels_over_point: null,
  
  // Used internally for horizontal graph types.
  has_left_labels: null,
  
  // A label for the bottom of the graph
  x_axis_label: null,
  
  // A label for the left side of the graph
  y_axis_label: null,
  
  // x_axis_increment: null,
  
  // Manually set increment of the horizontal marking lines
  y_axis_increment: null,
  
  // Get or set the list of colors that will be used to draw the bars or lines.
  colors: null,
  
  // The large title of the graph displayed at the top
  title: null,
  
  // Font used for titles, labels, etc.
  font: null,
  
  font_color: null,
  
  // Prevent drawing of line markers
  hide_line_markers: null,
  
  // Prevent drawing of the legend
  hide_legend: null,
  
  // Prevent drawing of the title
  hide_title: null,
  
  // Prevent drawing of line numbers
  hide_line_numbers: null,
  
  // Message shown when there is no data. Fits up to 20 characters. Defaults
  // to "No Data."
  no_data_message: null,
  
  // The font size of the large title at the top of the graph
  title_font_size: null,
  
  // Optionally set the size of the font. Based on an 800x600px graph.
  // Default is 20.
  //
  // Will be scaled down if graph is smaller than 800px wide.
  legend_font_size: null,
  
  // The font size of the labels around the graph
  marker_font_size: null,
  
  // The color of the auxiliary lines
  marker_color: null,
  
  // The number of horizontal lines shown for reference
  marker_count: null,
  
  // You can manually set a minimum value instead of having the values
  // guessed for you.
  //
  // Set it after you have given all your data to the graph object.
  minimum_value: null,
  
  // You can manually set a maximum value, such as a percentage-based graph
  // that always goes to 100.
  //
  // If you use this, you must set it after you have given all your data to
  // the graph object.
  maximum_value: null,
  
  // Set to false if you don't want the data to be sorted with largest avg
  // values at the back.
  sort: null,
  
  // Experimental
  additional_line_values: null,
  
  // Experimental
  stacked: null,
  
  // Optionally set the size of the colored box by each item in the legend.
  // Default is 20.0
  //
  // Will be scaled down if graph is smaller than 800px wide.
  legend_box_size: null,
  
  // Set to true to enable tooltip displays
  tooltips: false,
  
  // If one numerical argument is given, the graph is drawn at 4/3 ratio
  // according to the given width (800 results in 800x600, 400 gives 400x300,
  // etc.).
  //
  // Or, send a geometry string for other ratios ('800x400', '400x225').
  initialize: function(renderer, target_width) {
    this._d = new Bluff.Renderer(renderer);
    target_width = target_width || this.klass.DEFAULT_TARGET_WIDTH;
    
    var geo;
    
    if (typeof target_width !== 'number') {
      geo = target_width.split('x');
      this._columns = parseFloat(geo[0]);
      this._rows = parseFloat(geo[1]);
    } else {
      this._columns = parseFloat(target_width);
      this._rows = this._columns * 0.75;
    }
    
    this.initialize_ivars();
    
    this._reset_themes();
    this.theme_keynote();
    
    this._listeners = {};
  },
  
  // Set instance variables for this object.
  //
  // Subclasses can override this, call super, then set values separately.
  //
  // This makes it possible to set defaults in a subclass but still allow
  // developers to change this values in their program.
  initialize_ivars: function() {
    // Internal for calculations
    this._raw_columns = 800;
    this._raw_rows = 800 * (this._rows/this._columns);
    this._column_count = 0;
    this.marker_count = null;
    this.maximum_value = this.minimum_value = null;
    this._has_data = false;
    this._data = [];
    this.labels = {};
    this._labels_seen = {};
    this.sort = true;
    this.title = null;
    
    this._scale = this._columns / this._raw_columns;
    
    this.marker_font_size = 21.0;
    this.legend_font_size = 20.0;
    this.title_font_size = 36.0;
    
    this.top_margin = this.bottom_margin =
    this.left_margin = this.right_margin = this.klass.DEFAULT_MARGIN;
    
    this.legend_margin = this.klass.LEGEND_MARGIN;
    this.title_margin = this.klass.TITLE_MARGIN;
    
    this.legend_box_size = 20.0;
    
    this.no_data_message = "No Data";
    
    this.hide_line_markers = this.hide_legend = this.hide_title = this.hide_line_numbers = false;
    this.center_labels_over_point = true;
    this.has_left_labels = false;
    
    this.additional_line_values = [];
    this._additional_line_colors = [];
    this._theme_options = {};
    
    this.x_axis_label = this.y_axis_label = null;
    this.y_axis_increment = null;
    this.stacked = null;
    this._norm_data = null;
  },
  
  // Sets the top, bottom, left and right margins to +margin+.
  set_margins: function(margin) {
    this.top_margin = this.left_margin = this.right_margin = this.bottom_margin = margin;
  },
  
  // Sets the font for graph text to the font at +font_path+.
  set_font: function(font_path) {
    this.font = font_path;
    this._d.font = this.font;
  },
  
  // Add a color to the list of available colors for lines.
  //
  // Example:
  //  add_color('#c0e9d3')
  add_color: function(colorname) {
    this.colors.push(colorname);
  },
  
  // Replace the entire color list with a new array of colors. Also
  // aliased as the colors= setter method.
  //
  // If you specify fewer colors than the number of datasets you intend
  // to draw, 'increment_color' will cycle through the array, reusing
  // colors as needed.
  //
  // Note that (as with the 'set_theme' method), you should set up the color
  // list before you send your data (via the 'data' method). Calls to the
  // 'data' method made prior to this call will use whatever color scheme
  // was in place at the time data was called.
  //
  // Example:
  //  replace_colors ['#cc99cc', '#d9e043', '#34d8a2']
  replace_colors: function(color_list) {
    this.colors = color_list || [];
    this._color_index = 0;
  },
  
  // You can set a theme manually. Assign a hash to this method before you
  // send your data.
  //
  //  graph.set_theme({
  //    colors: ['orange', 'purple', 'green', 'white', 'red'],
  //    marker_color: 'blue',
  //    background_colors: ['black', 'grey']
  //  })
  //
  // background_image: 'squirrel.png' is also possible.
  //
  // (Or hopefully something better looking than that.)
  //
  set_theme: function(options) {
    this._reset_themes();
    
    this._theme_options = {
      colors: ['black', 'white'],
      additional_line_colors: [],
      marker_color: 'white',
      font_color: 'black',
      background_colors: null,
      background_image: null
    };
    for (var key in options) this._theme_options[key] = options[key];
    
    this.colors = this._theme_options.colors;
    this.marker_color = this._theme_options.marker_color;
    this.font_color = this._theme_options.font_color || this.marker_color;
    this._additional_line_colors = this._theme_options.additional_line_colors;
    
    this._render_background();
  },
  
  // Set just the background colors
  set_background: function(options) {
    if (options.colors)
      this._theme_options.background_colors = options.colors;
    if (options.image)
      this._theme_options.background_image = options.image;
    this._render_background();
  },
  
  // A color scheme similar to the popular presentation software.
  theme_keynote: function() {
    // Colors
    this._blue = '#6886B4';
    this._yellow = '#FDD84E';
    this._green = '#72AE6E';
    this._red = '#D1695E';
    this._purple = '#8A6EAF';
    this._orange = '#EFAA43';
    this._white = 'white';
    this.colors = [this._yellow, this._blue, this._green, this._red, this._purple, this._orange, this._white];
    
    this.set_theme({
      colors: this.colors,
      marker_color: 'white',
      font_color: 'white',
      background_colors: ['black', '#4a465a']
    });
  },
  
  // A color scheme plucked from the colors on the popular usability blog.
  theme_37signals: function() {
    // Colors
    this._green = '#339933';
    this._purple = '#cc99cc';
    this._blue = '#336699';
    this._yellow = '#FFF804';
    this._red = '#ff0000';
    this._orange = '#cf5910';
    this._black = 'black';
    this.colors = [this._yellow, this._blue, this._green, this._red, this._purple, this._orange, this._black];
    
    this.set_theme({
      colors: this.colors,
      marker_color: 'black',
      font_color: 'black',
      background_colors: ['#d1edf5', 'white']
    });
  },
  
  // A color scheme from the colors used on the 2005 Rails keynote
  // presentation at RubyConf.
  theme_rails_keynote: function() {
    // Colors
    this._green = '#00ff00';
    this._grey = '#333333';
    this._orange = '#ff5d00';
    this._red = '#f61100';
    this._white = 'white';
    this._light_grey = '#999999';
    this._black = 'black';
    this.colors = [this._green, this._grey, this._orange, this._red, this._white, this._light_grey, this._black];
    
    this.set_theme({
      colors: this.colors,
      marker_color: 'white',
      font_color: 'white',
      background_colors: ['#0083a3', '#0083a3']
    });
  },
  
  // A color scheme similar to that used on the popular podcast site.
  theme_odeo: function() {
    // Colors
    this._grey = '#202020';
    this._white = 'white';
    this._dark_pink = '#a21764';
    this._green = '#8ab438';
    this._light_grey = '#999999';
    this._dark_blue = '#3a5b87';
    this._black = 'black';
    this.colors = [this._grey, this._white, this._dark_blue, this._dark_pink, this._green, this._light_grey, this._black];
    
    this.set_theme({
      colors: this.colors,
      marker_color: 'white',
      font_color: 'white',
      background_colors: ['#ff47a4', '#ff1f81']
    });
  },
  
  // A pastel theme
  theme_pastel: function() {
    // Colors
    this.colors = [
                    '#a9dada', // blue
                    '#aedaa9', // green
                    '#daaea9', // peach
                    '#dadaa9', // yellow
                    '#a9a9da', // dk purple
                    '#daaeda', // purple
                    '#dadada' // grey
                  ];
    
    this.set_theme({
      colors: this.colors,
      marker_color: '#aea9a9', // Grey
      font_color: 'black',
      background_colors: 'white'
    });
  },
  
  // A greyscale theme
  theme_greyscale: function() {
    // Colors
    this.colors = [
                    '#282828', // 
                    '#383838', // 
                    '#686868', // 
                    '#989898', // 
                    '#c8c8c8', // 
                    '#e8e8e8' // 
                  ];
    
    this.set_theme({
      colors: this.colors,
      marker_color: '#aea9a9', // Grey
      font_color: 'black',
      background_colors: 'white'
    });
  },
  
  // Parameters are an array where the first element is the name of the dataset
  // and the value is an array of values to plot.
  //
  // Can be called multiple times with different datasets for a multi-valued
  // graph.
  //
  // If the color argument is nil, the next color from the default theme will
  // be used.
  //
  // NOTE: If you want to use a preset theme, you must set it before calling
  // data().
  //
  // Example:
  //   data("Bart S.", [95, 45, 78, 89, 88, 76], '#ffcc00')
  data: function(name, data_points, color) {
    data_points = (data_points === undefined) ? [] : data_points;
    color = color || null;
    
    data_points = Bluff.array(data_points); // make sure it's an array
    this._data.push([name, data_points, (color || this._increment_color())]);
    // Set column count if this is larger than previous counts
    this._column_count = (data_points.length > this._column_count) ? data_points.length : this._column_count;
    
    // Pre-normalize
    Bluff.each(data_points, function(data_point, index) {
      if (data_point === undefined) return;
      
      // Setup max/min so spread starts at the low end of the data points
      if (this.maximum_value === null && this.minimum_value === null)
        this.maximum_value = this.minimum_value = data_point;
      
      // TODO Doesn't work with stacked bar graphs
      // Original: @maximum_value = _larger_than_max?(data_point, index) ? max(data_point, index) : @maximum_value
      this.maximum_value = this._larger_than_max(data_point) ? data_point : this.maximum_value;
      if (this.maximum_value >= 0) this._has_data = true;
      
      this.minimum_value = this._less_than_min(data_point) ? data_point : this.minimum_value;
      if (this.minimum_value < 0) this._has_data = true;
    }, this);
  },
  
  // Overridden by subclasses to do the actual plotting of the graph.
  //
  // Subclasses should start by calling super() for this method.
  draw: function() {
    if (this.stacked) this._make_stacked();
    this._setup_drawing();
    
    this._debug(function() {
      // Outer margin
      this._d.rectangle(this.left_margin, this.top_margin,
                        this._raw_columns - this.right_margin, this._raw_rows - this.bottom_margin);
      // Graph area box
      this._d.rectangle(this._graph_left, this._graph_top, this._graph_right, this._graph_bottom);
    });
  },
  
  clear: function() {
    this._render_background();
  },
  
  on: function(eventType, callback, context) {
    var list = this._listeners[eventType] = this._listeners[eventType] || [];
    list.push([callback, context]);
  },
  
  trigger: function(eventType, data) {
    var list = this._listeners[eventType];
    if (!list) return;
    Bluff.each(list, function(listener) {
      listener[0].call(listener[1], data);
    });
  },
  
  // Calculates size of drawable area and draws the decorations.
  //
  // * line markers
  // * legend
  // * title
  _setup_drawing: function() {
    // Maybe should be done in one of the following functions for more granularity.
    if (!this._has_data) return this._draw_no_data();
    
    this._normalize();
    this._setup_graph_measurements();
    if (this.sort) this._sort_norm_data();
    
    this._draw_legend();
    this._draw_line_markers();
    this._draw_axis_labels();
    this._draw_title();
  },
  
  // Make copy of data with values scaled between 0-100
  _normalize: function(force) {
    if (this._norm_data === null || force === true) {
      this._norm_data = [];
      if (!this._has_data) return;
      
      this._calculate_spread();
      
      Bluff.each(this._data, function(data_row) {
        var norm_data_points = [];
        Bluff.each(data_row[this.klass.DATA_VALUES_INDEX], function(data_point) {
          if (data_point === null || data_point === undefined)
            norm_data_points.push(null);
          else
            norm_data_points.push((data_point - this.minimum_value) / this._spread);
        }, this);
        this._norm_data.push([data_row[this.klass.DATA_LABEL_INDEX], norm_data_points, data_row[this.klass.DATA_COLOR_INDEX]]);
      }, this);
    }
  },
  
  _calculate_spread: function() {
    this._spread = this.maximum_value - this.minimum_value;
    this._spread = this._spread > 0 ? this._spread : 1;
    
    var power = Math.round(Math.LOG10E*Math.log(this._spread));
    this._significant_digits = Math.pow(10, 3 - power);
  },
  
  // Calculates size of drawable area, general font dimensions, etc.
  _setup_graph_measurements: function() {
    this._marker_caps_height = this.hide_line_markers ? 0 :
      this._calculate_caps_height(this.marker_font_size);
    this._title_caps_height = this.hide_title ? 0 :
      this._calculate_caps_height(this.title_font_size);
    this._legend_caps_height = this.hide_legend ? 0 :
      this._calculate_caps_height(this.legend_font_size);
    
    var longest_label,
        longest_left_label_width,
        line_number_width,
        last_label,
        extra_room_for_long_label,
        x_axis_label_height,
        key;
    
    if (this.hide_line_markers) {
      this._graph_left = this.left_margin;
      this._graph_right_margin = this.right_margin;
      this._graph_bottom_margin = this.bottom_margin;
    } else {
      longest_left_label_width = 0;
      if (this.has_left_labels) {
        longest_label = '';
        for (key in this.labels) {
          longest_label = longest_label.length > this.labels[key].length
              ? longest_label
              : this.labels[key];
        }
        longest_left_label_width = this._calculate_width(this.marker_font_size, longest_label) * 1.25;
      } else {
        longest_left_label_width = this._calculate_width(this.marker_font_size, this._label(this.maximum_value));
      }
      
      // Shift graph if left line numbers are hidden
      line_number_width = this.hide_line_numbers && !this.has_left_labels ?
      0.0 :
        longest_left_label_width + this.klass.LABEL_MARGIN * 2;
      
      this._graph_left = this.left_margin +
        line_number_width +
        (this.y_axis_label === null ? 0.0 : this._marker_caps_height + this.klass.LABEL_MARGIN * 2);
      
      // Make space for half the width of the rightmost column label.
      // Might be greater than the number of columns if between-style bar markers are used.
      last_label = -Infinity;
      for (key in this.labels)
        last_label = last_label > Number(key) ? last_label : Number(key);
      last_label = Math.round(last_label);
      extra_room_for_long_label = (last_label >= (this._column_count-1) && this.center_labels_over_point) ?
      this._calculate_width(this.marker_font_size, this.labels[last_label]) / 2 :
        0;
      this._graph_right_margin  = this.right_margin + extra_room_for_long_label;
      
      this._graph_bottom_margin = this.bottom_margin +
        this._marker_caps_height + this.klass.LABEL_MARGIN;
    }
    
    this._graph_right = this._raw_columns - this._graph_right_margin;
    this._graph_width = this._raw_columns - this._graph_left - this._graph_right_margin;
    
    // When hide_title, leave a title_margin space for aesthetics.
    // Same with hide_legend
    this._graph_top = this.top_margin +
      (this.hide_title  ? this.title_margin  : this._title_caps_height  + this.title_margin ) +
      (this.hide_legend ? this.legend_margin : this._legend_caps_height + this.legend_margin);
    
    x_axis_label_height = (this.x_axis_label === null) ? 0.0 :
      this._marker_caps_height + this.klass.LABEL_MARGIN;
    this._graph_bottom = this._raw_rows - this._graph_bottom_margin - x_axis_label_height;
    this._graph_height = this._graph_bottom - this._graph_top;
  },
  
  // Draw the optional labels for the x axis and y axis.
  _draw_axis_labels: function() {
    if (this.x_axis_label) {
      // X Axis
      // Centered vertically and horizontally by setting the
      // height to 1.0 and the width to the width of the graph.
      var x_axis_label_y_coordinate = this._graph_bottom + this.klass.LABEL_MARGIN * 2 + this._marker_caps_height;
      
      // TODO Center between graph area
      this._d.fill = this.font_color;
      if (this.font) this._d.font = this.font;
      this._d.stroke = 'transparent';
      this._d.pointsize = this._scale_fontsize(this.marker_font_size);
      this._d.gravity = 'north';
      this._d.annotate_scaled(
                              this._raw_columns, 1.0,
                              0.0, x_axis_label_y_coordinate,
                              this.x_axis_label, this._scale);
      this._debug(function() {
        this._d.line(0.0, x_axis_label_y_coordinate, this._raw_columns, x_axis_label_y_coordinate);
      });
    }
    
    // TODO Y label (not generally possible in browsers)
  },
  
  // Draws horizontal background lines and labels
  _draw_line_markers: function() {
    if (this.hide_line_markers) return;
    
    if (this.y_axis_increment === null) {
      // Try to use a number of horizontal lines that will come out even.
      //
      // TODO Do the same for larger numbers...100, 75, 50, 25
      if (this.marker_count === null) {
        Bluff.each([3,4,5,6,7], function(lines) {
          if (!this.marker_count && this._spread % lines === 0)
            this.marker_count = lines;
        }, this);
        this.marker_count = this.marker_count || 4;
      }
      this._increment = (this._spread > 0) ? this._significant(this._spread / this.marker_count) : 1;
    } else {
      // TODO Make this work for negative values
      this.maximum_value = Math.max(Math.ceil(this.maximum_value), this.y_axis_increment);
      this.minimum_value = Math.floor(this.minimum_value);
      this._calculate_spread();
      this._normalize(true);
      
      this.marker_count = Math.round(this._spread / this.y_axis_increment);
      this._increment = this.y_axis_increment;
    }
    this._increment_scaled = this._graph_height / (this._spread / this._increment);
    
    // Draw horizontal line markers and annotate with numbers
    var index, n, y, marker_label;
    for (index = 0, n = this.marker_count; index <= n; index++) {
      y = this._graph_top + this._graph_height - index * this._increment_scaled;
      
      this._d.stroke = this.marker_color;
      this._d.stroke_width = 1;
      this._d.line(this._graph_left, y, this._graph_right, y);
      
      marker_label = index * this._increment + this.minimum_value;
      
      if (!this.hide_line_numbers) {
        this._d.fill = this.font_color;
        if (this.font) this._d.font = this.font;
        this._d.font_weight = 'normal';
        this._d.stroke = 'transparent';
        this._d.pointsize = this._scale_fontsize(this.marker_font_size);
        this._d.gravity = 'east';
        
        // Vertically center with 1.0 for the height
        this._d.annotate_scaled(this._graph_left - this.klass.LABEL_MARGIN,
                                1.0, 0.0, y,
                                this._label(marker_label), this._scale);
      }
    }
  },
  
  _center: function(size) {
    return (this._raw_columns - size) / 2;
  },
  
  // Draws a legend with the names of the datasets matched to the colors used
  // to draw them.
  _draw_legend: function() {
    if (this.hide_legend) return;
    
    this._legend_labels = Bluff.map(this._data, function(item) {
      return item[this.klass.DATA_LABEL_INDEX];
    }, this);
    
    var legend_square_width = this.legend_box_size; // small square with color of this item
    
    // May fix legend drawing problem at small sizes
    if (this.font) this._d.font = this.font;
    this._d.pointsize = this.legend_font_size;
    
    var label_widths = [[]]; // Used to calculate line wrap
    Bluff.each(this._legend_labels, function(label) {
      var last = label_widths.length - 1;
      var metrics = this._d.get_type_metrics(label);
      var label_width = metrics.width + legend_square_width * 2.7;
      label_widths[last].push(label_width);
      
      if (Bluff.sum(label_widths[last]) > (this._raw_columns * 0.9))
        label_widths.push([label_widths[last].pop()]);
    }, this);
    
    var current_x_offset = this._center(Bluff.sum(label_widths[0]));
    var current_y_offset = this.hide_title ?
    this.top_margin + this.title_margin :
      this.top_margin + this.title_margin + this._title_caps_height;
    
    this._debug(function() {
      this._d.stroke_width = 1;
      this._d.line(0, current_y_offset, this._raw_columns, current_y_offset);
    });
    
    Bluff.each(this._legend_labels, function(legend_label, index) {
      
      // Draw label
      this._d.fill = this.font_color;
      if (this.font) this._d.font = this.font;
      this._d.pointsize = this._scale_fontsize(this.legend_font_size);
      this._d.stroke = 'transparent';
      this._d.font_weight = 'normal';
      this._d.gravity = 'west';
      this._d.annotate_scaled(this._raw_columns, 1.0,
                              current_x_offset + (legend_square_width * 1.7), current_y_offset,
                              legend_label, this._scale);
      
      // Now draw box with color of this dataset
      this._d.stroke = 'transparent';
      this._d.fill = this._data[index][this.klass.DATA_COLOR_INDEX];
      this._d.rectangle(current_x_offset,
                        current_y_offset - legend_square_width / 2.0,
                        current_x_offset + legend_square_width,
                        current_y_offset + legend_square_width / 2.0);
      
      this._d.pointsize = this.legend_font_size;
      var metrics = this._d.get_type_metrics(legend_label);
      var current_string_offset = metrics.width + (legend_square_width * 2.7),
          line_height;
      
      // Handle wrapping
      label_widths[0].shift();
      if (label_widths[0].length == 0) {
        this._debug(function() {
          this._d.line(0.0, current_y_offset, this._raw_columns, current_y_offset);
        });
        
        label_widths.shift();
        if (label_widths.length > 0) current_x_offset = this._center(Bluff.sum(label_widths[0]));
        line_height = Math.max(this._legend_caps_height, legend_square_width) + this.legend_margin;
        if (label_widths.length > 0) {
          // Wrap to next line and shrink available graph dimensions
          current_y_offset += line_height;
          this._graph_top += line_height;
          this._graph_height = this._graph_bottom - this._graph_top;
        }
      } else {
        current_x_offset += current_string_offset;
      }
    }, this);
    this._color_index = 0;
  },
  
  // Draws a title on the graph.
  _draw_title: function() {
    if (this.hide_title || !this.title) return;
    
    this._d.fill = this.font_color;
    if (this.font) this._d.font = this.font;
    this._d.pointsize = this._scale_fontsize(this.title_font_size);
    this._d.font_weight = 'bold';
    this._d.gravity = 'north';
    this._d.annotate_scaled(this._raw_columns, 1.0,
                            0, this.top_margin,
                            this.title, this._scale);
  },
  
  // Draws column labels below graph, centered over x_offset
  //--
  // TODO Allow WestGravity as an option
  _draw_label: function(x_offset, index) {
    if (this.hide_line_markers) return;
    
    var y_offset;
    
    if (this.labels[index] && !this._labels_seen[index]) {
      y_offset = this._graph_bottom + this.klass.LABEL_MARGIN;
      
      this._d.fill = this.font_color;
      if (this.font) this._d.font = this.font;
      this._d.stroke = 'transparent';
      this._d.font_weight = 'normal';
      this._d.pointsize = this._scale_fontsize(this.marker_font_size);
      this._d.gravity = 'north';
      this._d.annotate_scaled(1.0, 1.0,
                              x_offset, y_offset,
                              this.labels[index], this._scale);
      this._labels_seen[index] = true;
      
      this._debug(function() {
        this._d.stroke_width = 1;
        this._d.line(0.0, y_offset, this._raw_columns, y_offset);
      });
    }
  },
  
  // Creates a mouse hover target rectangle for tooltip displays
  _draw_tooltip: function(left, top, width, height, name, color, data, index) {
    if (!this.tooltips) return;
    var node = this._d.tooltip(left, top, width, height, name, color, data);
    
    Bluff.Event.observe(node, 'click', function() {
      var point = {
        series: name,
        label:  this.labels[index],
        value:  data,
        color:  color
      };
      this.trigger('click:datapoint', point);
    }, this);
  },
  
  // Shows an error message because you have no data.
  _draw_no_data: function() {
    this._d.fill = this.font_color;
    if (this.font) this._d.font = this.font;
    this._d.stroke = 'transparent';
    this._d.font_weight = 'normal';
    this._d.pointsize = this._scale_fontsize(80);
    this._d.gravity = 'center';
    this._d.annotate_scaled(this._raw_columns, this._raw_rows/2,
                            0, 10,
                            this.no_data_message, this._scale);
  },
  
  // Finds the best background to render based on the provided theme options.
  _render_background: function() {
    var colors = this._theme_options.background_colors;
    switch (true) {
      case colors instanceof Array:
        this._render_gradiated_background.apply(this, colors);
        break;
      case typeof colors === 'string':
        this._render_solid_background(colors);
        break;
      default:
        this._render_image_background(this._theme_options.background_image);
        break;
    }
  },
  
  // Make a new image at the current size with a solid +color+.
  _render_solid_background: function(color) {
    this._d.render_solid_background(this._columns, this._rows, color);
  },
  
  // Use with a theme definition method to draw a gradiated background.
  _render_gradiated_background: function(top_color, bottom_color) {
    this._d.render_gradiated_background(this._columns, this._rows, top_color, bottom_color);
  },
  
  // Use with a theme to use an image (800x600 original) background.
  _render_image_background: function(image_path) {
    // TODO
  },
  
  // Resets everything to defaults (except data).
  _reset_themes: function() {
    this._color_index = 0;
    this._labels_seen = {};
    this._theme_options = {};
    this._d.scale(this._scale, this._scale);
  },
  
  _scale_value: function(value) {
    return this._scale * value;
  },
  
  // Return a comparable fontsize for the current graph.
  _scale_fontsize: function(value) {
    var new_fontsize = value * this._scale;
    return new_fontsize;
  },
  
  _clip_value_if_greater_than: function(value, max_value) {
    return (value > max_value) ? max_value : value;
  },
  
  // Overridden by subclasses such as stacked bar.
  _larger_than_max: function(data_point, index) {
    return data_point > this.maximum_value;
  },
  
  _less_than_min: function(data_point, index) {
    return data_point < this.minimum_value;
  },
  
  // Overridden by subclasses that need it.
  _max: function(data_point, index) {
    return data_point;
  },
  
  // Overridden by subclasses that need it.
  _min: function(data_point, index) {
    return data_point;
  },
  
  _significant: function(inc) {
    if (inc == 0) return 1.0;
    var factor = 1.0;
    while (inc < 10) {
      inc *= 10;
      factor /= 10;
    }
    
    while (inc > 100) {
      inc /= 10;
      factor *= 10;
    }
    
    return Math.floor(inc) * factor;
  },
  
  // Sort with largest overall summed value at front of array so it shows up
  // correctly in the drawn graph.
  _sort_norm_data: function() {
    var sums = this._sums, index = this.klass.DATA_VALUES_INDEX;
    
    this._norm_data.sort(function(a,b) {
      return sums(b[index]) - sums(a[index]);
    });
    
    this._data.sort(function(a,b) {
      return sums(b[index]) - sums(a[index]);
    });
  },
  
  _sums: function(data_set) {
    var total_sum = 0;
    Bluff.each(data_set, function(num) { total_sum += (num || 0) });
    return total_sum;
  },
  
  _make_stacked: function() {
    var stacked_values = [], i = this._column_count;
    while (i--) stacked_values[i] = 0;
    Bluff.each(this._data, function(value_set) {
      Bluff.each(value_set[this.klass.DATA_VALUES_INDEX], function(value, index) {
        stacked_values[index] += value;
      }, this);
      value_set[this.klass.DATA_VALUES_INDEX] = Bluff.array(stacked_values);
    }, this);
  },
  
  // Takes a block and draws it if DEBUG is true.
  //
  // Example:
  //   debug { @d.rectangle x1, y1, x2, y2 }
  _debug: function(block) {
    if (this.klass.DEBUG) {
      this._d.fill = 'transparent';
      this._d.stroke = 'turquoise';
      block.call(this);
    }
  },
  
  // Returns the next color in your color list.
  _increment_color: function() {
    var offset = this._color_index;
    this._color_index = (this._color_index + 1) % this.colors.length;
    return this.colors[offset];
  },
  
  // Return a formatted string representing a number value that should be
  // printed as a label.
  _label: function(value) {
    var sep   = this.klass.THOUSAND_SEPARATOR,
        label = (this._spread % this.marker_count == 0 || this.y_axis_increment !== null)
        ? String(Math.round(value))
        : String(Math.floor(value * this._significant_digits)/this._significant_digits);
    
    var parts = label.split('.');
    parts[0] = parts[0].replace(/(\d)(?=(\d\d\d)+(?!\d))/g, '$1' + sep);
    return parts.join('.');
  },
  
  // Returns the height of the capital letter 'X' for the current font and
  // size.
  //
  // Not scaled since it deals with dimensions that the regular scaling will
  // handle.
  _calculate_caps_height: function(font_size) {
    return this._d.caps_height(font_size);
  },
  
  // Returns the width of a string at this pointsize.
  //
  // Not scaled since it deals with dimensions that the regular 
  // scaling will handle.
  _calculate_width: function(font_size, text) {
    return this._d.text_width(font_size, text);
  }
});


Bluff.Area = new JS.Class(Bluff.Base, {
  
  draw: function() {
    this.callSuper();
    
    if (!this._has_data) return;
    
    this._x_increment = this._graph_width / (this._column_count - 1);
    this._d.stroke = 'transparent';
    
    Bluff.each(this._norm_data, function(data_row) {
      var poly_points = [],
          prev_x = 0.0,
          prev_y = 0.0;
      
      Bluff.each(data_row[this.klass.DATA_VALUES_INDEX], function(data_point, index) {
        // Use incremented x and scaled y
        var new_x = this._graph_left + (this._x_increment * index);
        var new_y = this._graph_top + (this._graph_height - data_point * this._graph_height);
        
        if (prev_x > 0 && prev_y > 0) {
          poly_points.push(new_x);
          poly_points.push(new_y);
          
          // this._d.polyline(prev_x, prev_y, new_x, new_y);
        } else {
          poly_points.push(this._graph_left);
          poly_points.push(this._graph_bottom - 1);
          poly_points.push(new_x);
          poly_points.push(new_y);
          
          // this._d.polyline(this._graph_left, this._graph_bottom, new_x, new_y);
        }
        
        this._draw_label(new_x, index);
        
        prev_x = new_x;
        prev_y = new_y;
      }, this);
      
      // Add closing points, draw polygon
      poly_points.push(this._graph_right);
      poly_points.push(this._graph_bottom - 1);
      poly_points.push(this._graph_left);
      poly_points.push(this._graph_bottom - 1);
      
      this._d.fill = data_row[this.klass.DATA_COLOR_INDEX];
      this._d.polyline(poly_points);
      
    }, this);
  }
});


//  This class perfoms the y coordinats conversion for the bar class.
//
//  There are three cases: 
//
//    1. Bars all go from zero in positive direction
//    2. Bars all go from zero to negative direction  
//    3. Bars either go from zero to positive or from zero to negative
//
Bluff.BarConversion = new JS.Class({
  mode:           null,
  zero:           null,
  graph_top:      null,
  graph_height:   null,
  minimum_value:  null,
  spread:         null,
  
  getLeftYRightYscaled: function(data_point, result) {
    var val;
    switch (this.mode) {
      case 1: // Case one
        // minimum value >= 0 ( only positiv values )
        result[0] = this.graph_top + this.graph_height*(1 - data_point) + 1;
        result[1] = this.graph_top + this.graph_height - 1;
        break;
      case 2:  // Case two
        // only negativ values
         result[0] = this.graph_top + 1;
        result[1] = this.graph_top + this.graph_height*(1 - data_point) - 1;
        break;
      case 3: // Case three
        // positiv and negativ values
        val = data_point-this.minimum_value/this.spread;
        if ( data_point >= this.zero ) {
          result[0] = this.graph_top + this.graph_height*(1 - (val-this.zero)) + 1;
          result[1] = this.graph_top + this.graph_height*(1 - this.zero) - 1;
        } else {
          result[0] = this.graph_top + this.graph_height*(1 - (val-this.zero)) + 1;
          result[1] = this.graph_top + this.graph_height*(1 - this.zero) - 1;
        }
        break;
      default:
        result[0] = 0.0;
        result[1] = 0.0;
    }        
  }  
  
});


Bluff.Bar = new JS.Class(Bluff.Base, {
  
  // Spacing factor applied between bars
  bar_spacing: 0.9,
  
  draw: function() {
    // Labels will be centered over the left of the bar if
    // there are more labels than columns. This is basically the same 
    // as where it would be for a line graph.
    this.center_labels_over_point = (Bluff.keys(this.labels).length > this._column_count);
    
    this.callSuper();
    if (!this._has_data) return;
    
    this._draw_bars();
  },
  
  _draw_bars: function() {
    this._bar_width = this._graph_width / (this._column_count * this._data.length);
    var padding = (this._bar_width * (1 - this.bar_spacing)) / 2;
    
    this._d.stroke_opacity = 0.0;
    
    // Setup the BarConversion Object
    var conversion = new Bluff.BarConversion();
    conversion.graph_height = this._graph_height;
    conversion.graph_top = this._graph_top;
    
    // Set up the right mode [1,2,3] see BarConversion for further explanation
    if (this.minimum_value >= 0) {
      // all bars go from zero to positiv
      conversion.mode = 1;
    } else {
      // all bars go from 0 to negativ
      if (this.maximum_value <= 0) {
        conversion.mode = 2;
      } else {
        // bars either go from zero to negativ or to positiv
        conversion.mode = 3;
        conversion.spread = this._spread;
        conversion.minimum_value = this.minimum_value;
        conversion.zero = -this.minimum_value/this._spread;
      }
    }
    
    // iterate over all normalised data
    Bluff.each(this._norm_data, function(data_row, row_index) {
      var raw_data = this._data[row_index][this.klass.DATA_VALUES_INDEX];
      
      Bluff.each(data_row[this.klass.DATA_VALUES_INDEX], function(data_point, point_index) {
        // Use incremented x and scaled y
        // x
        var left_x = this._graph_left + (this._bar_width * (row_index + point_index + ((this._data.length - 1) * point_index))) + padding;
        var right_x = left_x + this._bar_width * this.bar_spacing;
        // y
        var conv = [];
        conversion.getLeftYRightYscaled(data_point, conv);
        
        // create new bar
        this._d.fill = data_row[this.klass.DATA_COLOR_INDEX];
        this._d.rectangle(left_x, conv[0], right_x, conv[1]);
        
        // create tooltip target
        this._draw_tooltip(left_x, conv[0],
                           right_x - left_x, conv[1] - conv[0],
                           data_row[this.klass.DATA_LABEL_INDEX],
                           data_row[this.klass.DATA_COLOR_INDEX],
                           raw_data[point_index], point_index);
        
        // Calculate center based on bar_width and current row
        var label_center = this._graph_left + 
                          (this._data.length * this._bar_width * point_index) + 
                          (this._data.length * this._bar_width / 2.0);
        // Subtract half a bar width to center left if requested
        this._draw_label(label_center - (this.center_labels_over_point ? this._bar_width / 2.0 : 0.0), point_index);
      }, this);
      
    }, this);
    
    // Draw the last label if requested
    if (this.center_labels_over_point) this._draw_label(this._graph_right, this._column_count);
  }
});


// Here's how to make a Line graph:
//
//   g = new Bluff.Line('canvasId');
//   g.title = "A Line Graph";
//   g.data('Fries', [20, 23, 19, 8]);
//   g.data('Hamburgers', [50, 19, 99, 29]);
//   g.draw();
//
// There are also other options described below, such as #baseline_value, #baseline_color, #hide_dots, and #hide_lines.

Bluff.Line = new JS.Class(Bluff.Base, {
  // Draw a dashed line at the given value
  baseline_value: null,
  
  // Color of the baseline
  baseline_color: null,
  
  // Dimensions of lines and dots; calculated based on dataset size if left unspecified
  line_width: null,
  dot_radius: null,
  
  // Hide parts of the graph to fit more datapoints, or for a different appearance.
  hide_dots: null,
  hide_lines: null,
  
  // Call with target pixel width of graph (800, 400, 300), and/or 'false' to omit lines (points only).
  //
  //  g = new Bluff.Line('canvasId', 400) // 400px wide with lines
  //
  //  g = new Bluff.Line('canvasId', 400, false) // 400px wide, no lines (for backwards compatibility)
  //
  //  g = new Bluff.Line('canvasId', false) // Defaults to 800px wide, no lines (for backwards compatibility)
  // 
  // The preferred way is to call hide_dots or hide_lines instead.
  initialize: function(renderer) {
    if (arguments.length > 3) throw 'Wrong number of arguments';
    if (arguments.length === 1 || (typeof arguments[1] !== 'number' && typeof arguments[1] !== 'string'))
      this.callSuper(renderer, null);
    else
      this.callSuper();
    
    this.hide_dots = this.hide_lines = false;
    this.baseline_color = 'red';
    this.baseline_value = null;
  },
  
  draw: function() {
    this.callSuper();
    
    if (!this._has_data) return;
    
    // Check to see if more than one datapoint was given. NaN can result otherwise.
    this.x_increment = (this._column_count > 1) ? (this._graph_width / (this._column_count - 1)) : this._graph_width;
    
    var level;
    
    if (this._norm_baseline !== undefined) {
      level = this._graph_top + (this._graph_height - this._norm_baseline * this._graph_height);
      this._d.push();
      this._d.stroke = this.baseline_color;
      this._d.fill_opacity = 0.0;
      // this._d.stroke_dasharray(10, 20);
      this._d.stroke_width = 3.0;
      this._d.line(this._graph_left, level, this._graph_left + this._graph_width, level);
      this._d.pop();
    }
    
    Bluff.each(this._norm_data, function(data_row, row_index) {
      var prev_x = null, prev_y = null;
      var raw_data = this._data[row_index][this.klass.DATA_VALUES_INDEX];
      
      this._one_point = this._contains_one_point_only(data_row);
      
      Bluff.each(data_row[this.klass.DATA_VALUES_INDEX], function(data_point, index) {
        var new_x = this._graph_left + (this.x_increment * index);
        if (typeof data_point !== 'number') return;
        
        this._draw_label(new_x, index);
        
        var new_y = this._graph_top + (this._graph_height - data_point * this._graph_height);
        
        // Reset each time to avoid thin-line errors
        this._d.stroke = data_row[this.klass.DATA_COLOR_INDEX];
        this._d.fill = data_row[this.klass.DATA_COLOR_INDEX];
        this._d.stroke_opacity = 1.0;
        this._d.stroke_width = this.line_width ||
          this._clip_value_if_greater_than(this._columns / (this._norm_data[0][this.klass.DATA_VALUES_INDEX].length * 6), 3.0);
        
        var circle_radius = this.dot_radius ||
          this._clip_value_if_greater_than(this._columns / (this._norm_data[0][this.klass.DATA_VALUES_INDEX].length * 2), 7.0);
        
        if (!this.hide_lines && prev_x !== null && prev_y !== null) {
          this._d.line(prev_x, prev_y, new_x, new_y);
        } else if (this._one_point) {
          // Show a circle if there's just one point
          this._d.circle(new_x, new_y, new_x - circle_radius, new_y);
        }
        
        if (!this.hide_dots) this._d.circle(new_x, new_y, new_x - circle_radius, new_y);
        
        this._draw_tooltip(new_x - circle_radius, new_y - circle_radius,
                           2 * circle_radius, 2 *circle_radius,
                           data_row[this.klass.DATA_LABEL_INDEX],
                           data_row[this.klass.DATA_COLOR_INDEX],
                           raw_data[index], index);
        
        prev_x = new_x;
        prev_y = new_y;
      }, this);
    }, this);
  },
  
  _normalize: function() {
    this.maximum_value = Math.max(this.maximum_value, this.baseline_value);
    this.callSuper();
    if (this.baseline_value !== null) this._norm_baseline = this.baseline_value / this.maximum_value;
  },
  
  _contains_one_point_only: function(data_row) {
    // Spin through data to determine if there is just one value present.
    var count = 0;
    Bluff.each(data_row[this.klass.DATA_VALUES_INDEX], function(data_point) {
      if (data_point !== undefined) count += 1;
    });
    return count === 1;
  }
});


// Graph with dots and labels along a vertical access
// see: 'Creating More Effective Graphs' by Robbins

Bluff.Dot = new JS.Class(Bluff.Base, {
  
  draw: function() {
    this.has_left_labels = true;
    this.callSuper();
    
    if (!this._has_data) return;
    
    // Setup spacing.
    //
    var spacing_factor = 1.0;
    
    this._items_width = this._graph_height / this._column_count;
    this._item_width = this._items_width * spacing_factor / this._norm_data.length;
    this._d.stroke_opacity = 0.0;
    var height = Bluff.array_new(this._column_count, 0),
        length = Bluff.array_new(this._column_count, this._graph_left),
        padding = (this._items_width * (1 - spacing_factor)) / 2;
    
    Bluff.each(this._norm_data, function(data_row, row_index) {
      Bluff.each(data_row[this.klass.DATA_VALUES_INDEX], function(data_point, point_index) {
        
        var x_pos = this._graph_left + (data_point * this._graph_width) - Math.round(this._item_width/6.0);
        var y_pos = this._graph_top + (this._items_width * point_index) + padding + Math.round(this._item_width/2.0);
        
        if (row_index === 0) {
          this._d.stroke = this.marker_color;
          this._d.stroke_width = 1.0;
          this._d.opacity = 0.1;
          this._d.line(this._graph_left, y_pos, this._graph_left + this._graph_width, y_pos);
        }
        
        this._d.fill = data_row[this.klass.DATA_COLOR_INDEX];
        this._d.stroke = 'transparent';
        this._d.circle(x_pos, y_pos, x_pos + Math.round(this._item_width/3.0), y_pos);
        
        // Calculate center based on item_width and current row
        var label_center = this._graph_top + (this._items_width * point_index + this._items_width / 2) + padding;
        this._draw_label(label_center, point_index);
      }, this);
      
    }, this);
  },
  
  // Instead of base class version, draws vertical background lines and label
  _draw_line_markers: function() {
    
    if (this.hide_line_markers) return;
    
    this._d.stroke_antialias = false;
    
    // Draw horizontal line markers and annotate with numbers
    this._d.stroke_width = 1;
    var number_of_lines = 5;
    
    // TODO Round maximum marker value to a round number like 100, 0.1, 0.5, etc.
    var increment = this._significant(this.maximum_value / number_of_lines);
    for (var index = 0; index <= number_of_lines; index++) {
      
      var line_diff    = (this._graph_right - this._graph_left) / number_of_lines,
          x            = this._graph_right - (line_diff * index) - 1,
          diff         = index - number_of_lines,
          marker_label = Math.abs(diff) * increment;
      
      this._d.stroke = this.marker_color;
      this._d.line(x, this._graph_bottom, x, this._graph_bottom + 0.5 * this.klass.LABEL_MARGIN);
      
      if (!this.hide_line_numbers) {
        this._d.fill      = this.font_color;
        if (this.font) this._d.font = this.font;
        this._d.stroke    = 'transparent';
        this._d.pointsize = this._scale_fontsize(this.marker_font_size);
        this._d.gravity   = 'center';
        // TODO Center text over line
        this._d.annotate_scaled(0, 0, // Width of box to draw text in
                                x, this._graph_bottom + (this.klass.LABEL_MARGIN * 2.0), // Coordinates of text
                                marker_label, this._scale);
      }
      this._d.stroke_antialias = true;
    }
  },
  
  // Draw on the Y axis instead of the X
  _draw_label: function(y_offset, index) {
    if (this.labels[index] && !this._labels_seen[index]) {
      this._d.fill             = this.font_color;
      if (this.font) this._d.font = this.font;
      this._d.stroke           = 'transparent';
      this._d.font_weight      = 'normal';
      this._d.pointsize        = this._scale_fontsize(this.marker_font_size);
      this._d.gravity          = 'east';
      this._d.annotate_scaled(1, 1,
                              this._graph_left - this.klass.LABEL_MARGIN * 2.0, y_offset,
                              this.labels[index], this._scale);
      this._labels_seen[index] = true;
    }
  }
});


// Experimental!!! See also the Spider graph.
Bluff.Net = new JS.Class(Bluff.Base, {
  
  // Hide parts of the graph to fit more datapoints, or for a different appearance.
  hide_dots: null,
  
  //Dimensions of lines and dots; calculated based on dataset size if left unspecified
  line_width: null,
  dot_radius: null,
  
  initialize: function() {
    this.callSuper();
    
    this.hide_dots = false;
    this.hide_line_numbers = true;
  },
  
  draw: function() {
    
    this.callSuper();
    
    if (!this._has_data) return;
    
    this._radius = this._graph_height / 2.0;
    this._center_x = this._graph_left + (this._graph_width / 2.0);
    this._center_y = this._graph_top + (this._graph_height / 2.0) - 10; // Move graph up a bit
    
    this._x_increment = this._graph_width / (this._column_count - 1);
    var circle_radius = this.dot_radius ||
      this._clip_value_if_greater_than(this._columns / (this._norm_data[0][this.klass.DATA_VALUES_INDEX].length * 2.5), 7.0);
    
    this._d.stroke_opacity = 1.0;
    this._d.stroke_width = this.line_width ||
      this._clip_value_if_greater_than(this._columns / (this._norm_data[0][this.klass.DATA_VALUES_INDEX].length * 4), 3.0);
    
    var level;
    
    if (this._norm_baseline !== undefined) {
      level = this._graph_top + (this._graph_height - this._norm_baseline * this._graph_height);
      this._d.push();
      this._d.stroke_color  = this.baseline_color;
      this._d.fill_opacity = 0.0;
      // this._d.stroke_dasharray(10, 20);
      this._d.stroke_width = 5;
      this._d.line(this._graph_left, level, this._graph_left + this._graph_width, level);
      this._d.pop();
    }
    
    Bluff.each(this._norm_data, function(data_row) {
      var prev_x = null, prev_y = null;
      
      Bluff.each(data_row[this.klass.DATA_VALUES_INDEX], function(data_point, index) {
        if (data_point === undefined) return;
        
        var rad_pos = index * Math.PI * 2 / this._column_count,
            point_distance = data_point * this._radius,
            start_x = this._center_x + Math.sin(rad_pos) * point_distance,
            start_y = this._center_y - Math.cos(rad_pos) * point_distance,
            
            next_index = (index + 1 < data_row[this.klass.DATA_VALUES_INDEX].length) ? index + 1 : 0,
            
            next_rad_pos = next_index * Math.PI * 2 / this._column_count,
            next_point_distance = data_row[this.klass.DATA_VALUES_INDEX][next_index] * this._radius,
            end_x = this._center_x + Math.sin(next_rad_pos) * next_point_distance,
            end_y = this._center_y - Math.cos(next_rad_pos) * next_point_distance;
        
        this._d.stroke = data_row[this.klass.DATA_COLOR_INDEX];
        this._d.fill = data_row[this.klass.DATA_COLOR_INDEX];
        this._d.line(start_x, start_y, end_x, end_y);
        
        if (!this.hide_dots) this._d.circle(start_x, start_y, start_x - circle_radius, start_y);
      }, this);
      
    }, this);
  },
  
  // the lines connecting in the center, with the first line vertical
  _draw_line_markers: function() {
    if (this.hide_line_markers) return;
    
    // have to do this here (AGAIN)... see draw() in this class
    // because this funtion is called before the @radius, @center_x and @center_y are set
    this._radius = this._graph_height / 2.0;
    this._center_x = this._graph_left + (this._graph_width / 2.0);
    this._center_y = this._graph_top + (this._graph_height / 2.0) - 10; // Move graph up a bit
    
    var rad_pos, marker_label;
    
    for (var index = 0, n = this._column_count; index < n; index++) {
      rad_pos = index * Math.PI * 2 / this._column_count;
      
      // Draw horizontal line markers and annotate with numbers
      this._d.stroke = this.marker_color;
      this._d.stroke_width = 1;
      
      this._d.line(this._center_x, this._center_y, this._center_x + Math.sin(rad_pos) * this._radius, this._center_y - Math.cos(rad_pos) * this._radius);
      
      marker_label = this.labels[index] ? this.labels[index] : '000';
      
      this._draw_label(this._center_x, this._center_y, rad_pos * 360 / (2 * Math.PI), this._radius, marker_label);
    }
  },
  
  _draw_label: function(center_x, center_y, angle, radius, amount) {
    var r_offset = 1.1,
        x_offset = center_x, // + 15 // The label points need to be tweaked slightly
        y_offset = center_y, // + 0  // This one doesn't though
        rad_pos = angle * Math.PI / 180,
        x = x_offset + (radius * r_offset * Math.sin(rad_pos)),
        y = y_offset - (radius * r_offset * Math.cos(rad_pos));
    
    // Draw label
    this._d.fill = this.marker_color;
    if (this.font) this._d.font = this.font;
    this._d.pointsize = this._scale_fontsize(20);
    this._d.stroke = 'transparent';
    this._d.font_weight = 'bold';
    this._d.gravity = 'center';
    this._d.annotate_scaled(0, 0, x, y, amount, this._scale);
  }
});


// Here's how to make a Pie graph:
//
//   g = new Bluff.Pie('canvasId');
//   g.title = "Visual Pie Graph Test";
//   g.data('Fries', 20);
//   g.data('Hamburgers', 50);
//   g.draw();
//
// To control where the pie chart starts creating slices, use #zero_degree.

Bluff.Pie = new JS.Class(Bluff.Base, {
  extend: {
    TEXT_OFFSET_PERCENTAGE: 0.08
  },
  
  // Can be used to make the pie start cutting slices at the top (-90.0)
  // or at another angle. Default is 0.0, which starts at 3 o'clock.
  zero_degreee: null,
  
  // Do not show labels for slices that are less than this percent. Use 0 to always show all labels.
  hide_labels_less_than: null,
  
  initialize_ivars: function() {
    this.callSuper();
    this.zero_degree = 0.0;
    this.hide_labels_less_than = 0.0;
  },
  
  draw: function() {
    this.hide_line_markers = true;
    
    this.callSuper();
    
    if (!this._has_data) return;
    
    var diameter = this._graph_height,
        radius = (Math.min(this._graph_width, this._graph_height) / 2.0) * 0.8,
        top_x = this._graph_left + (this._graph_width - diameter) / 2.0,
        center_x = this._graph_left + (this._graph_width / 2.0),
        center_y = this._graph_top + (this._graph_height / 2.0) - 10, // Move graph up a bit
        total_sum = this._sums_for_pie(),
        prev_degrees = this.zero_degree,
        index = this.klass.DATA_VALUES_INDEX;
    
    // Use full data since we can easily calculate percentages
    if (this.sort) this._data.sort(function(a,b) { return a[index][0] - b[index][0]; });
    Bluff.each(this._data, function(data_row, i) {
      if (data_row[this.klass.DATA_VALUES_INDEX][0] > 0) {
        this._d.fill = data_row[this.klass.DATA_COLOR_INDEX];
        
        var current_degrees = (data_row[this.klass.DATA_VALUES_INDEX][0] / total_sum) * 360;
        
        // Gruff uses ellipse() here, but canvas doesn't seem to support it.
        // circle() is fine for our purposes here.
        this._d.circle(center_x, center_y,
                    center_x + radius, center_y,
                    prev_degrees, prev_degrees + current_degrees + 0.5); // <= +0.5 'fudge factor' gets rid of the ugly gaps
        
        var half_angle = prev_degrees + ((prev_degrees + current_degrees) - prev_degrees) / 2,
            label_val = Math.round((data_row[this.klass.DATA_VALUES_INDEX][0] / total_sum) * 100.0),
            label_string;
        
        if (label_val >= this.hide_labels_less_than) {
          label_string = this._label(data_row[this.klass.DATA_VALUES_INDEX][0]);
          this._draw_label(center_x, center_y, half_angle,
                            radius + (radius * this.klass.TEXT_OFFSET_PERCENTAGE),
                            label_string,
                            data_row, i);
        }
        
        prev_degrees += current_degrees;
      }
    }, this);
    
    // TODO debug a circle where the text is drawn...
  },
  
  // Labels are drawn around a slightly wider ellipse to give room for 
  // labels on the left and right.
  _draw_label: function(center_x, center_y, angle, radius, amount, data_row, i) {
    // TODO Don't use so many hard-coded numbers
    var r_offset = 20.0,      // The distance out from the center of the pie to get point
        x_offset = center_x,  // + 15.0 # The label points need to be tweaked slightly
        y_offset = center_y,  // This one doesn't though
        radius_offset = radius + r_offset,
        ellipse_factor = radius_offset * 0.15,
        x = x_offset + ((radius_offset + ellipse_factor) * Math.cos(angle * Math.PI/180)),
        y = y_offset + (radius_offset * Math.sin(angle * Math.PI/180));
    
    // Draw label
    this._d.fill = this.font_color;
    if (this.font) this._d.font = this.font;
    this._d.pointsize = this._scale_fontsize(this.marker_font_size);
    this._d.font_weight = 'bold';
    this._d.gravity = 'center';
    this._d.annotate_scaled(0,0, x,y, amount, this._scale);
    
    this._draw_tooltip(x - 20, y - 20, 40, 40,
                       data_row[this.klass.DATA_LABEL_INDEX],
                       data_row[this.klass.DATA_COLOR_INDEX],
                       amount, i);
  },
  
  _sums_for_pie: function() {
    var total_sum = 0;
    Bluff.each(this._data, function(data_row) {
      total_sum += data_row[this.klass.DATA_VALUES_INDEX][0];
    }, this);
    return total_sum;
  }
});


// Graph with individual horizontal bars instead of vertical bars.

Bluff.SideBar = new JS.Class(Bluff.Base, {
  
  // Spacing factor applied between bars
  bar_spacing: 0.9,
  
  draw: function() {
    this.has_left_labels = true;
    this.callSuper();
    
    if (!this._has_data) return;
    this._draw_bars();
  },
  
  _draw_bars: function() {
    this._bars_width       = this._graph_height / this._column_count;
    this._bar_width        = this._bars_width / this._norm_data.length;
    this._d.stroke_opacity = 0.0;
    var height = Bluff.array_new(this._column_count, 0),
        length = Bluff.array_new(this._column_count, this._graph_left),
        padding = (this._bar_width * (1 - this.bar_spacing)) / 2;
    
    Bluff.each(this._norm_data, function(data_row, row_index) {
      var raw_data = this._data[row_index][this.klass.DATA_VALUES_INDEX];
      Bluff.each(data_row[this.klass.DATA_VALUES_INDEX], function(data_point, point_index) {
        
        // Using the original calcs from the stacked bar chart
        // to get the difference between
        // part of the bart chart we wish to stack.
        var temp1      = this._graph_left + (this._graph_width - data_point * this._graph_width - height[point_index]),
            temp2      = this._graph_left + this._graph_width - height[point_index],
            difference = temp2 - temp1,
        
            left_x     = length[point_index] - 1,
            left_y     = this._graph_top + (this._bars_width * point_index) + (this._bar_width * row_index) + padding,
            right_x    = left_x + difference,
            right_y    = left_y + this._bar_width * this.bar_spacing;
        
        height[point_index] += (data_point * this._graph_width);
        
        this._d.stroke = 'transparent';
        this._d.fill = data_row[this.klass.DATA_COLOR_INDEX];
        this._d.rectangle(left_x, left_y, right_x, right_y);
        
        this._draw_tooltip(left_x, left_y,
                           right_x - left_x, right_y - left_y,
                           data_row[this.klass.DATA_LABEL_INDEX],
                           data_row[this.klass.DATA_COLOR_INDEX],
                           raw_data[point_index], point_index);
        
        // Calculate center based on bar_width and current row
        var label_center = this._graph_top + (this._bars_width * point_index + this._bars_width / 2);
        this._draw_label(label_center, point_index);
      }, this)
      
    }, this);
  },
  
  // Instead of base class version, draws vertical background lines and label
  _draw_line_markers: function() {
    
    if (this.hide_line_markers) return;
    
    this._d.stroke_antialias = false;
    
    // Draw horizontal line markers and annotate with numbers
    this._d.stroke_width = 1;
    var number_of_lines = 5;
    
    // TODO Round maximum marker value to a round number like 100, 0.1, 0.5, etc.
    var increment = this._significant(this._spread / number_of_lines),
        line_diff, x, diff, marker_label;
    for (var index = 0; index <= number_of_lines; index++) {
      
      line_diff    = (this._graph_right - this._graph_left) / number_of_lines;
      x            = this._graph_right - (line_diff * index) - 1;
      diff         = index - number_of_lines;
      marker_label = Math.abs(diff) * increment + this.minimum_value;
      
      this._d.stroke = this.marker_color;
      this._d.line(x, this._graph_bottom, x, this._graph_top);
      
      if (!this.hide_line_numbers) {
        this._d.fill      = this.font_color;
        if (this.font) this._d.font = this.font;
        this._d.stroke    = 'transparent';
        this._d.pointsize = this._scale_fontsize(this.marker_font_size);
        this._d.gravity   = 'center';
        // TODO Center text over line
        this._d.annotate_scaled(
                          0, 0, // Width of box to draw text in
                          x, this._graph_bottom + (this.klass.LABEL_MARGIN * 2.0), // Coordinates of text
                          this._label(marker_label), this._scale);
      }
    }
  },
  
  // Draw on the Y axis instead of the X
  _draw_label: function(y_offset, index) {
    if (this.labels[index] && !this._labels_seen[index]) {
      this._d.fill             = this.font_color;
      if (this.font) this._d.font = this.font;
      this._d.stroke           = 'transparent';
      this._d.font_weight      = 'normal';
      this._d.pointsize        = this._scale_fontsize(this.marker_font_size);
      this._d.gravity          = 'east';
      this._d.annotate_scaled(1, 1,
                              this._graph_left - this.klass.LABEL_MARGIN * 2.0, y_offset,
                              this.labels[index], this._scale);
      this._labels_seen[index] = true;
    }
  }
});


// Experimental!!! See also the Net graph.
//
// Submitted by Kevin Clark http://glu.ttono.us/
Bluff.Spider = new JS.Class(Bluff.Base, {
  
  // Hide all text
  hide_text: null,
  hide_axes: null,
  transparent_background: null,
  
  initialize: function(renderer, max_value, target_width) {
    this.callSuper(renderer, target_width);
    this._max_value = max_value;
    this.hide_legend = true;
  },
  
  draw: function() {
    this.hide_line_markers = true;
    
    this.callSuper();
    
    if (!this._has_data) return;
    
    // Setup basic positioning
    var diameter = this._graph_height,
        radius = this._graph_height / 2.0,
        top_x = this._graph_left + (this._graph_width - diameter) / 2.0,
        center_x = this._graph_left + (this._graph_width / 2.0),
        center_y = this._graph_top + (this._graph_height / 2.0) - 25; // Move graph up a bit
    
    this._unit_length = radius / this._max_value;
    
    var total_sum = this._sums_for_spider(),
        prev_degrees = 0.0,
        additive_angle = (2 * Math.PI) / this._data.length,
        
        current_angle = 0.0;
    
    // Draw axes
    if (!this.hide_axes) this._draw_axes(center_x, center_y, radius, additive_angle);
    
    // Draw polygon
    this._draw_polygon(center_x, center_y, additive_angle);
  },
  
  _normalize_points: function(value) {
    return value * this._unit_length;
  },
  
  _draw_label: function(center_x, center_y, angle, radius, amount) {
    var r_offset = 50,            // The distance out from the center of the pie to get point
        x_offset = center_x,      // The label points need to be tweaked slightly
        y_offset = center_y + 0,  // This one doesn't though
        x = x_offset + ((radius + r_offset) * Math.cos(angle)),
        y = y_offset + ((radius + r_offset) * Math.sin(angle));
    
    // Draw label
    this._d.fill = this.marker_color;
    if (this.font) this._d.font = this.font;
    this._d.pointsize = this._scale_fontsize(this.legend_font_size);
    this._d.stroke = 'transparent';
    this._d.font_weight = 'bold';
    this._d.gravity = 'center';
    this._d.annotate_scaled(0, 0,
                            x, y,
                            amount, this._scale);
  },
  
  _draw_axes: function(center_x, center_y, radius, additive_angle, line_color) {
    if (this.hide_axes) return;
    
    var current_angle = 0.0;
    
    Bluff.each(this._data, function(data_row) {
      this._d.stroke = line_color || data_row[this.klass.DATA_COLOR_INDEX];
      this._d.stroke_width = 5.0;
      
      var x_offset = radius * Math.cos(current_angle);
      var y_offset = radius * Math.sin(current_angle);
      
      this._d.line(center_x, center_y,
                   center_x + x_offset,
                   center_y + y_offset);
      
      if (!this.hide_text) this._draw_label(center_x, center_y, current_angle, radius, data_row[this.klass.DATA_LABEL_INDEX]);
      
      current_angle += additive_angle;
    }, this);
  },
  
  _draw_polygon: function(center_x, center_y, additive_angle, color) {
    var points = [],
        current_angle = 0.0;
    Bluff.each(this._data, function(data_row) {
      points.push(center_x + this._normalize_points(data_row[this.klass.DATA_VALUES_INDEX][0]) * Math.cos(current_angle));
      points.push(center_y + this._normalize_points(data_row[this.klass.DATA_VALUES_INDEX][0]) * Math.sin(current_angle));
      current_angle += additive_angle;
    }, this);
    
    this._d.stroke_width = 1.0;
    this._d.stroke = color || this.marker_color;
    this._d.fill = color || this.marker_color;
    this._d.fill_opacity = 0.4;
    this._d.polyline(points);
  },
  
  _sums_for_spider: function() {
    var sum = 0.0;
    Bluff.each(this._data, function(data_row) {
      sum += data_row[this.klass.DATA_VALUES_INDEX][0];
    }, this);
    return sum;
  }
});


// Used by StackedBar and child classes.
Bluff.Base.StackedMixin = new JS.Module({
  // Get sum of each stack
  _get_maximum_by_stack: function() {
    var max_hash = {};
    Bluff.each(this._data, function(data_set) {
      Bluff.each(data_set[this.klass.DATA_VALUES_INDEX], function(data_point, i) {
        if (!max_hash[i]) max_hash[i] = 0.0;
        max_hash[i] += data_point;
      }, this);
    }, this);
    
    // this.maximum_value = 0;
    for (var key in max_hash) {
      if (max_hash[key] > this.maximum_value) this.maximum_value = max_hash[key];
    }
    this.minimum_value = 0;
  }
});


Bluff.StackedArea = new JS.Class(Bluff.Base, {
  include: Bluff.Base.StackedMixin,
  last_series_goes_on_bottom: null,
  
  draw: function() {
    this._get_maximum_by_stack();
    this.callSuper();
    
    if (!this._has_data) return;
    
    this._x_increment = this._graph_width / (this._column_count - 1);
    this._d.stroke = 'transparent';
    
    var height = Bluff.array_new(this._column_count, 0);
    
    var data_points = null;
    var iterator = this.last_series_goes_on_bottom ? 'reverse_each' : 'each';
    Bluff[iterator](this._norm_data, function(data_row) {
      var prev_data_points = data_points;
      data_points = [];
      
      Bluff.each(data_row[this.klass.DATA_VALUES_INDEX], function(data_point, index) {
        // Use incremented x and scaled y
        var new_x = this._graph_left + (this._x_increment * index);
        var new_y = this._graph_top + (this._graph_height - data_point * this._graph_height - height[index]);
        
        height[index] += (data_point * this._graph_height);
        
        data_points.push(new_x);
        data_points.push(new_y);
        
        this._draw_label(new_x, index);
      }, this);
      
      var poly_points, i, n;
      
      if (prev_data_points) {
        poly_points = Bluff.array(data_points);
        for (i = prev_data_points.length/2 - 1; i >= 0; i--) {
          poly_points.push(prev_data_points[2*i]);
          poly_points.push(prev_data_points[2*i+1]);
        }
        poly_points.push(data_points[0]);
        poly_points.push(data_points[1]);
      } else {
        poly_points = Bluff.array(data_points);
        poly_points.push(this._graph_right);
        poly_points.push(this._graph_bottom - 1);
        poly_points.push(this._graph_left);
        poly_points.push(this._graph_bottom - 1);
        poly_points.push(data_points[0]);
        poly_points.push(data_points[1]);
      }
      this._d.fill = data_row[this.klass.DATA_COLOR_INDEX];
      this._d.polyline(poly_points);
    }, this);
  }
});


Bluff.StackedBar = new JS.Class(Bluff.Base, {
  include: Bluff.Base.StackedMixin,
  
  // Spacing factor applied between bars
  bar_spacing: 0.9,
  
  // Draws a bar graph, but multiple sets are stacked on top of each other.
  draw: function() {
    this._get_maximum_by_stack();
    this.callSuper();
    if (!this._has_data) return;
    
    this._bar_width = this._graph_width / this._column_count;
    var padding = (this._bar_width * (1 - this.bar_spacing)) / 2;
    
    this._d.stroke_opacity = 0.0;
    
    var height = Bluff.array_new(this._column_count, 0);
    
    Bluff.each(this._norm_data, function(data_row, row_index) {
      var raw_data = this._data[row_index][this.klass.DATA_VALUES_INDEX];
      
      Bluff.each(data_row[this.klass.DATA_VALUES_INDEX], function(data_point, point_index) {
        // Calculate center based on bar_width and current row
        var label_center = this._graph_left + (this._bar_width * point_index) + (this._bar_width * this.bar_spacing / 2.0);
        this._draw_label(label_center, point_index);
        
        if (data_point == 0) return;
        // Use incremented x and scaled y
        var left_x = this._graph_left + (this._bar_width * point_index) + padding;
        var left_y = this._graph_top + (this._graph_height -
                                        data_point * this._graph_height - 
                                        height[point_index]) + 1;
        var right_x = left_x + this._bar_width * this.bar_spacing;
        var right_y = this._graph_top + this._graph_height - height[point_index] - 1;
        
        // update the total height of the current stacked bar
        height[point_index] += (data_point * this._graph_height);
        
        this._d.fill = data_row[this.klass.DATA_COLOR_INDEX];
        this._d.rectangle(left_x, left_y, right_x, right_y);
        
        this._draw_tooltip(left_x, left_y,
                           right_x - left_x, right_y - left_y,
                           data_row[this.klass.DATA_LABEL_INDEX],
                           data_row[this.klass.DATA_COLOR_INDEX],
                           raw_data[point_index], point_index);
      }, this);
    }, this);
  }
});


// A special bar graph that shows a single dataset as a set of
// stacked bars. The bottom bar shows the running total and 
// the top bar shows the new value being added to the array.

Bluff.AccumulatorBar = new JS.Class(Bluff.StackedBar, {
  
  draw: function() {
    if (this._data.length !== 1) throw 'Incorrect number of datasets';
    
    var accumulator_array = [],
        index = 0,
        increment_array = [];
    
    Bluff.each(this._data[0][this.klass.DATA_VALUES_INDEX], function(value) {
      var max = -Infinity;
      Bluff.each(increment_array, function(x) { max = Math.max(max, x); });
      
      increment_array.push((index > 0) ? (value + max) : value);
      accumulator_array.push(increment_array[index] - value);
      index += 1;
    }, this);
    
    this.data("Accumulator", accumulator_array);
    
    this.callSuper();
  }
});


// New gruff graph type added to enable sideways stacking bar charts 
// (basically looks like a x/y flip of a standard stacking bar chart)
//
// alun.eyre@googlemail.com

Bluff.SideStackedBar = new JS.Class(Bluff.SideBar, {
  include: Bluff.Base.StackedMixin,
  
  // Spacing factor applied between bars
  bar_spacing: 0.9,
  
  draw: function() {
    this.has_left_labels = true;
    this._get_maximum_by_stack();
    this.callSuper();
  },
  
  _draw_bars: function() {
    this._bar_width = this._graph_height / this._column_count;
    var height = Bluff.array_new(this._column_count, 0),
        length = Bluff.array_new(this._column_count, this._graph_left),
        padding = (this._bar_width * (1 - this.bar_spacing)) / 2;

    Bluff.each(this._norm_data, function(data_row, row_index) {
      var raw_data = this._data[row_index][this.klass.DATA_VALUES_INDEX];
      
      Bluff.each(data_row[this.klass.DATA_VALUES_INDEX], function(data_point, point_index) {
        
        // using the original calcs from the stacked bar chart to get the difference between
        // part of the bart chart we wish to stack.
        var temp1 = this._graph_left + (this._graph_width -
                                            data_point * this._graph_width - 
                                            height[point_index]) + 1;
        var temp2 = this._graph_left + this._graph_width - height[point_index] - 1;
        var difference = temp2 - temp1;
        
        this._d.fill = data_row[this.klass.DATA_COLOR_INDEX];
        
        var left_x = length[point_index], //+ 1
            left_y = this._graph_top + (this._bar_width * point_index) + padding,
            right_x = left_x + difference,
            right_y = left_y + this._bar_width * this.bar_spacing;
        length[point_index] += difference;
        height[point_index] += (data_point * this._graph_width - 2);
        
        this._d.rectangle(left_x, left_y, right_x, right_y);
        
        this._draw_tooltip(left_x, left_y,
                           right_x - left_x, right_y - left_y,
                           data_row[this.klass.DATA_LABEL_INDEX],
                           data_row[this.klass.DATA_COLOR_INDEX],
                           raw_data[point_index], point_index);
        
        // Calculate center based on bar_width and current row
        var label_center = this._graph_top + (this._bar_width * point_index) + (this._bar_width * this.bar_spacing / 2.0);
        this._draw_label(label_center, point_index);
      }, this);
    }, this);
  },
  
  _larger_than_max: function(data_point, index) {
    index = index || 0;
    return this._max(data_point, index) > this.maximum_value;
  },
  
  _max: function(data_point, index) {
    var sum = 0;
    Bluff.each(this._data, function(item) {
      sum += item[this.klass.DATA_VALUES_INDEX][index];
    }, this);
    return sum;
  }
});


Bluff.Mini.Legend = new JS.Module({
  
  hide_mini_legend: false,
  
  // The canvas needs to be bigger so we can put the legend beneath it.
  _expand_canvas_for_vertical_legend: function() {
    if (this.hide_mini_legend) return;
    
    this._legend_labels = Bluff.map(this._data, function(item) {
      return item[this.klass.DATA_LABEL_INDEX];
    }, this);
    
    var legend_height = this._scale_fontsize(
                          this._data.length * this._calculate_line_height() +
                          this.top_margin + this.bottom_margin);
    
    this._original_rows = this._raw_rows;
    this._original_columns = this._raw_columns;
    
    switch (this.legend_position) {
      case 'right':
        this._rows = Math.max(this._rows, legend_height);
        this._columns += this._calculate_legend_width() + this.left_margin;
        break;
      
      default:
        this._rows += legend_height;
        break;
    }
    this._render_background();
  },
  
  _calculate_line_height: function() {
    return this._calculate_caps_height(this.legend_font_size) * 1.7;
  },
  
  _calculate_legend_width: function() {
    var width = 0;
    Bluff.each(this._legend_labels, function(label) {
      width = Math.max(this._calculate_width(this.legend_font_size, label), width);
    }, this);
    return this._scale_fontsize(width + 40*1.7);
  },
  
  // Draw the legend beneath the existing graph.
  _draw_vertical_legend: function() {
    if (this.hide_mini_legend) return;
    
    var legend_square_width = 40.0, // small square with color of this item
        legend_square_margin = 10.0,
        legend_left_margin = 100.0,
        legend_top_margin = 40.0;
    
    // May fix legend drawing problem at small sizes
    if (this.font) this._d.font = this.font;
    this._d.pointsize = this.legend_font_size;
    
    var current_x_offset, current_y_offset;
    
    switch (this.legend_position) {
      case 'right':
        current_x_offset = this._original_columns + this.left_margin;
        current_y_offset = this.top_margin + legend_top_margin;
        break;
      
      default:
        current_x_offset = legend_left_margin,
        current_y_offset = this._original_rows + legend_top_margin;
        break;
    }
    
    this._debug(function() {
      this._d.line(0.0, current_y_offset, this._raw_columns, current_y_offset);
    });
    
    Bluff.each(this._legend_labels, function(legend_label, index) {
      
      // Draw label
      this._d.fill = this.font_color;
      if (this.font) this._d.font = this.font;
      this._d.pointsize = this._scale_fontsize(this.legend_font_size);
      this._d.stroke = 'transparent';
      this._d.font_weight = 'normal';
      this._d.gravity = 'west';
      this._d.annotate_scaled(this._raw_columns, 1.0,
                        current_x_offset + (legend_square_width * 1.7), current_y_offset, 
                        this._truncate_legend_label(legend_label), this._scale);
      
      // Now draw box with color of this dataset
      this._d.stroke = 'transparent';
      this._d.fill = this._data[index][this.klass.DATA_COLOR_INDEX];
      this._d.rectangle(current_x_offset, 
                        current_y_offset - legend_square_width / 2.0, 
                        current_x_offset + legend_square_width, 
                        current_y_offset + legend_square_width / 2.0);
      
      current_y_offset += this._calculate_line_height();
    }, this);
    this._color_index = 0;
  },
  
  // Shorten long labels so they will fit on the canvas.
  _truncate_legend_label: function(label) {
    var truncated_label = String(label);
    while (this._calculate_width(this._scale_fontsize(this.legend_font_size), truncated_label) > (this._columns - this.legend_left_margin - this.right_margin) && (truncated_label.length > 1))
      truncated_label = truncated_label.substr(0, truncated_label.length-1);
    return truncated_label + (truncated_label.length < String(label).length ? "..." : '');
  }
});


// Makes a small bar graph suitable for display at 200px or even smaller.
//
Bluff.Mini.Bar = new JS.Class(Bluff.Bar, {
  include: Bluff.Mini.Legend,
  
  initialize_ivars: function() {
    this.callSuper();
    
    this.hide_legend = true;
    this.hide_title = true;
    this.hide_line_numbers = true;
    
    this.marker_font_size = 50.0;
    this.minimum_value = 0.0;
    this.maximum_value = 0.0;
    this.legend_font_size = 60.0;
  },
  
  draw: function() {
    this._expand_canvas_for_vertical_legend();
    
    this.callSuper();
    
    this._draw_vertical_legend();
  }
});


// Makes a small pie graph suitable for display at 200px or even smaller.
//
Bluff.Mini.Pie = new JS.Class(Bluff.Pie, {
  include: Bluff.Mini.Legend,
  
  initialize_ivars: function() {
    this.callSuper();
    
    this.hide_legend = true;
    this.hide_title = true;
    this.hide_line_numbers = true;
    
    this.marker_font_size = 60.0;
    this.legend_font_size = 60.0;
  },
  
  draw: function() {
    this._expand_canvas_for_vertical_legend();
    
    this.callSuper();
    
    this._draw_vertical_legend();
  }
});


// Makes a small pie graph suitable for display at 200px or even smaller.
//
Bluff.Mini.SideBar = new JS.Class(Bluff.SideBar, {
  include: Bluff.Mini.Legend,
  
  initialize_ivars: function() {
    this.callSuper();
    this.hide_legend = true;
    this.hide_title = true;
    this.hide_line_numbers = true;
    
    this.marker_font_size = 50.0;
    this.legend_font_size = 50.0;
  },
  
  draw: function() {
    this._expand_canvas_for_vertical_legend();
    
    this.callSuper();
    
    this._draw_vertical_legend();
  }
});


Bluff.Renderer = new JS.Class({
  extend: {
    WRAPPER_CLASS:  'bluff-wrapper',
    TEXT_CLASS:     'bluff-text',
    TARGET_CLASS:   'bluff-tooltip-target'
  },

  font:     'Arial, Helvetica, Verdana, sans-serif',
  gravity:  'north',
  
  initialize: function(canvasId) {
    this._canvas = document.getElementById(canvasId);
    this._ctx = this._canvas.getContext('2d');
  },
  
  scale: function(sx, sy) {
    this._sx = sx;
    this._sy = sy || sx;
  },
  
  caps_height: function(font_size) {
    var X = this._sized_text(font_size, 'X'),
        height = this._element_size(X).height;
    this._remove_node(X);
    return height;
  },
  
  text_width: function(font_size, text) {
    var element = this._sized_text(font_size, text);
    var width = this._element_size(element).width;
    this._remove_node(element);
    return width;
  },
  
  get_type_metrics: function(text) {
    var node = this._sized_text(this.pointsize, text);
    document.body.appendChild(node);
    var size = this._element_size(node);
    this._remove_node(node);
    return size;
  },
  
  clear: function(width, height) {
    this._canvas.width = width;
    this._canvas.height = height;
    this._ctx.clearRect(0, 0, width, height);
    var wrapper = this._text_container(), children = wrapper.childNodes, i = children.length;
    wrapper.style.width = width + 'px';
    wrapper.style.height = height + 'px';
    while (i--) {
      if (children[i].tagName.toLowerCase() !== 'canvas') {
        Bluff.Event.stopObserving(children[i]);
        this._remove_node(children[i]);
      }
    }
  },
  
  push: function() {
    this._ctx.save();
  },
  
  pop: function() {
    this._ctx.restore();
  },
  
  render_gradiated_background: function(width, height, top_color, bottom_color) {
    this.clear(width, height);
    var gradient = this._ctx.createLinearGradient(0,0, 0,height);
    gradient.addColorStop(0, top_color);
    gradient.addColorStop(1, bottom_color);
    this._ctx.fillStyle = gradient;
    this._ctx.fillRect(0, 0, width, height);
  },
  
  render_solid_background: function(width, height, color) {
    this.clear(width, height);
    this._ctx.fillStyle = color;
    this._ctx.fillRect(0, 0, width, height);
  },
  
  annotate_scaled: function(width, height, x, y, text, scale) {
    var scaled_width = (width * scale) >= 1 ? (width * scale) : 1;
    var scaled_height = (height * scale) >= 1 ? (height * scale) : 1;
    var text = this._sized_text(this.pointsize, text);
    text.style.color = this.fill;
    text.style.cursor = 'default';
    text.style.fontWeight = this.font_weight;
    text.style.textAlign = 'center';
    text.style.left = (this._sx * x + this._left_adjustment(text, scaled_width)) + 'px';
    text.style.top = (this._sy * y + this._top_adjustment(text, scaled_height)) + 'px';
  },
  
  tooltip: function(left, top, width, height, name, color, data) {
    if (width < 0) left += width;
    if (height < 0) top += height;
    
    var wrapper = this._canvas.parentNode,
        target = document.createElement('div');
    target.className = this.klass.TARGET_CLASS;
    target.style.cursor = 'default';
    target.style.position = 'absolute';
    target.style.left = (this._sx * left - 3) + 'px';
    target.style.top = (this._sy * top - 3) + 'px';
    target.style.width = (this._sx * Math.abs(width) + 5) + 'px';
    target.style.height = (this._sy * Math.abs(height) + 5) + 'px';
    target.style.fontSize = 0;
    target.style.overflow = 'hidden';
    
    Bluff.Event.observe(target, 'mouseover', function(node) {
      Bluff.Tooltip.show(name, color, data);
    });
    Bluff.Event.observe(target, 'mouseout', function(node) {
      Bluff.Tooltip.hide();
    });
    
    wrapper.appendChild(target);
    return target;
  },
  
  circle: function(origin_x, origin_y, perim_x, perim_y, arc_start, arc_end) {
    var radius = Math.sqrt(Math.pow(perim_x - origin_x, 2) + Math.pow(perim_y - origin_y, 2));
    var alpha = 0, beta = 2 * Math.PI; // radians to full circle
    
    this._ctx.fillStyle = this.fill;
    this._ctx.beginPath();
    
    if (arc_start !== undefined && arc_end !== undefined &&
        Math.abs(Math.floor(arc_end - arc_start)) !== 360) {
      alpha = arc_start * Math.PI/180;
      beta  = arc_end   * Math.PI/180;
      
      this._ctx.moveTo(this._sx * (origin_x + radius * Math.cos(beta)), this._sy * (origin_y + radius * Math.sin(beta)));
      this._ctx.lineTo(this._sx * origin_x, this._sy * origin_y);
      this._ctx.lineTo(this._sx * (origin_x + radius * Math.cos(alpha)), this._sy * (origin_y + radius * Math.sin(alpha)));
    }
    this._ctx.arc(this._sx * origin_x, this._sy * origin_y, this._sx * radius, alpha, beta, false); // draw it clockwise
    this._ctx.fill();
  },
  
  line: function(sx, sy, ex, ey) {
    this._ctx.strokeStyle = this.stroke;
    this._ctx.lineWidth = this.stroke_width;
    this._ctx.beginPath();
    this._ctx.moveTo(this._sx * sx, this._sy * sy);
    this._ctx.lineTo(this._sx * ex, this._sy * ey);
    this._ctx.stroke();
  },
  
  polyline: function(points) {
    this._ctx.fillStyle = this.fill;
    this._ctx.globalAlpha = this.fill_opacity || 1;
    try { this._ctx.strokeStyle = this.stroke; } catch (e) {}
    var x = points.shift(), y = points.shift();
    this._ctx.beginPath();
    this._ctx.moveTo(this._sx * x, this._sy * y);
    while (points.length > 0) {
      x = points.shift(); y = points.shift();
      this._ctx.lineTo(this._sx * x, this._sy * y);
    }
    this._ctx.fill();
  },
  
  rectangle: function(ax, ay, bx, by) {
    var temp;
    if (ax > bx) { temp = ax; ax = bx; bx = temp; }
    if (ay > by) { temp = ay; ay = by; by = temp; }
    try {
      this._ctx.fillStyle = this.fill;
      this._ctx.fillRect(this._sx * ax, this._sy * ay, this._sx * (bx-ax), this._sy * (by-ay));
    } catch (e) {}
    try {
      this._ctx.strokeStyle = this.stroke;
      if (this.stroke !== 'transparent')
        this._ctx.strokeRect(this._sx * ax, this._sy * ay, this._sx * (bx-ax), this._sy * (by-ay));
    } catch (e) {}
  },
  
  _left_adjustment: function(node, width) {
    var w = this._element_size(node).width;
    switch (this.gravity) {
      case 'west':    return 0;
      case 'east':    return width - w;
      case 'north': case 'south': case 'center':
        return (width - w) / 2;
    }
  },
  
  _top_adjustment: function(node, height) {
    var h = this._element_size(node).height;
    switch (this.gravity) {
      case 'north':   return 0;
      case 'south':   return height - h;
      case 'west': case 'east': case 'center':
        return (height - h) / 2;
    }
  },
  
  _text_container: function() {
    var wrapper = this._canvas.parentNode;
    if (wrapper.className === this.klass.WRAPPER_CLASS) return wrapper;
    wrapper = document.createElement('div');
    wrapper.className = this.klass.WRAPPER_CLASS;
    
    wrapper.style.position = 'relative';
    wrapper.style.border = 'none';
    wrapper.style.padding = '0 0 0 0';
    
    this._canvas.parentNode.insertBefore(wrapper, this._canvas);
    wrapper.appendChild(this._canvas);
    return wrapper;
  },
  
  _sized_text: function(size, content) {
    var text = this._text_node(content);
    text.style.fontFamily = this.font;
    text.style.fontSize = (typeof size === 'number') ? size + 'px' : size;
    return text;
  },
  
  _text_node: function(content) {
    var div = document.createElement('div');
    div.className = this.klass.TEXT_CLASS;
    div.style.position = 'absolute';
    div.appendChild(document.createTextNode(content));
    this._text_container().appendChild(div);
    return div;
  },
  
  _remove_node: function(node) {
    node.parentNode.removeChild(node);
    if (node.className === this.klass.TARGET_CLASS)
      Bluff.Event.stopObserving(node);
  },
  
  _element_size: function(element) {
    var display = element.style.display;
    return (display && display !== 'none')
        ? {width: element.offsetWidth, height: element.offsetHeight}
        : {width: element.clientWidth, height: element.clientHeight};
  }
});


// DOM event module, adapted from Prototype
// Copyright (c) 2005-2008 Sam Stephenson

Bluff.Event = {
  _cache: [],
  
  _isIE: (window.attachEvent && navigator.userAgent.indexOf('Opera') === -1),
  
  observe: function(element, eventName, callback, scope) {
    var handlers = Bluff.map(this._handlersFor(element, eventName),
                      function(entry) { return entry._handler });
    if (Bluff.index(handlers, callback) !== -1) return;
    
    var responder = function(event) {
      callback.call(scope || null, element, Bluff.Event._extend(event));
    };
    this._cache.push({_node: element, _name: eventName,
                      _handler: callback, _responder: responder});
    
    if (element.addEventListener)
      element.addEventListener(eventName, responder, false);
    else
      element.attachEvent('on' + eventName, responder);
  },
  
  stopObserving: function(element) {
    var handlers = element ? this._handlersFor(element) : this._cache;
    Bluff.each(handlers, function(entry) {
      if (entry._node.removeEventListener)
        entry._node.removeEventListener(entry._name, entry._responder, false);
      else
        entry._node.detachEvent('on' + entry._name, entry._responder);
    });
  },
  
  _handlersFor: function(element, eventName) {
    var results = [];
    Bluff.each(this._cache, function(entry) {
      if (element && entry._node !== element) return;
      if (eventName && entry._name !== eventName) return;
      results.push(entry);
    });
    return results;
  },
  
  _extend: function(event) {
    if (!this._isIE) return event;
    if (!event) return false;
    if (event._extendedByBluff) return event;
    event._extendedByBluff = true;
    
    var pointer = this._pointer(event);
    event.target = event.srcElement;
    event.pageX = pointer.x;
    event.pageY = pointer.y;
    
    return event;
  },
  
  _pointer: function(event) {
    var docElement = document.documentElement,
        body = document.body || { scrollLeft: 0, scrollTop: 0 };
    return {
      x: event.pageX || (event.clientX +
                        (docElement.scrollLeft || body.scrollLeft) -
                        (docElement.clientLeft || 0)),
      y: event.pageY || (event.clientY +
                        (docElement.scrollTop || body.scrollTop) -
                        (docElement.clientTop || 0))
    };
  }
};

if (Bluff.Event._isIE)
  window.attachEvent('onunload', function() {
    Bluff.Event.stopObserving();
    Bluff.Event._cache = null;
  });

if (navigator.userAgent.indexOf('AppleWebKit/') > -1)
  window.addEventListener('unload', function() {}, false);


Bluff.Tooltip = new JS.Singleton({
  LEFT_OFFSET:  20,
  TOP_OFFSET:   -6,
  DATA_LENGTH:  8,
  
  CLASS_NAME:   'bluff-tooltip',
  
  setup: function() {
    this._tip = document.createElement('div');
    this._tip.className = this.CLASS_NAME;
    this._tip.style.position = 'absolute';
    this.hide();
    document.body.appendChild(this._tip);
    
    Bluff.Event.observe(document.body, 'mousemove', function(body, event) {
      this._tip.style.left = (event.pageX + this.LEFT_OFFSET) + 'px';
      this._tip.style.top = (event.pageY + this.TOP_OFFSET) + 'px';
    }, this);
  },
  
  show: function(name, color, data) {
    data = Number(String(data).substr(0, this.DATA_LENGTH));
    this._tip.innerHTML = '<span class="color" style="background: ' + color + ';">&nbsp;</span> ' +
                          '<span class="label">' + name + '</span> ' +
                          '<span class="data">' + data + '</span>';
    this._tip.style.display = '';
  },
  
  hide: function() {
    this._tip.style.display = 'none';
  }
});

Bluff.Event.observe(window, 'load', Bluff.Tooltip.method('setup'));


Bluff.TableReader = new JS.Class({
  
  NUMBER_FORMAT: /\-?(0|[1-9]\d*)(\.\d+)?(e[\+\-]?\d+)?/i,
  
  initialize: function(table, options) {
    this._options = options || {};
    this._orientation = this._options.orientation || 'auto';
    
    this._table = (typeof table === 'string')
        ? document.getElementById(table)
        : table;
  },
  
  // Get array of data series from the table
  get_data: function() {
    if (!this._data) this._read();
    return this._data;
  },
  
  // Get set of axis labels to use for the graph
  get_labels: function() {
    if (!this._labels) this._read();
    return this._labels;
  },
  
  // Get the title from the table's caption
  get_title: function() {
    return this._title;
  },
  
  // Return series number i
  get_series: function(i) {
    if (this._data[i]) return this._data[i];
    return this._data[i] = {points: []};
  },
  
  // Gather data by reading from the table
  _read: function() {
    this._row = this._col = 0;
    this._row_offset = this._col_offset = 0;
    this._data = [];
    this._labels = {};
    this._row_headings = [];
    this._col_headings = [];
    this._skip_rows = [];
    this._skip_cols = [];
    
    this._walk(this._table);
    this._cleanup();
    this._orient();
    
    Bluff.each(this._col_headings, function(heading, i) {
      this.get_series(i - this._col_offset).name = heading;
    }, this);
    
    Bluff.each(this._row_headings, function(heading, i) {
      this._labels[i - this._row_offset] = heading;
    }, this);
  },
  
  // Walk the table's DOM tree
  _walk: function(node) {
    this._visit(node);
    var i, children = node.childNodes, n = children.length;
    for (i = 0; i < n; i++) this._walk(children[i]);
  },
  
  // Read a single DOM node from the table
  _visit: function(node) {
    if (!node.tagName) return;
    var content = this._strip_tags(node.innerHTML), x, y;
    switch (node.tagName.toUpperCase()) {
    
      case 'TR':
        if (!this._has_data) this._row_offset = this._row;
        this._row += 1;
        this._col = 0;
        break;
      
      case 'TD':
        if (!this._has_data) this._col_offset = this._col;
        this._has_data = true;
        this._col += 1;
        content = content.match(this.NUMBER_FORMAT);
        if (content === null) {
          this.get_series(x).points[y] = null;
        } else {
          x = this._col - this._col_offset - 1;
          y = this._row - this._row_offset - 1;
          this.get_series(x).points[y] = parseFloat(content[0]);
        }
        break;
      
      case 'TH':
        this._col += 1;
        if (this._ignore(node)) {
          this._skip_cols.push(this._col);
          this._skip_rows.push(this._row);
        }
        if (this._col === 1 && this._row === 1)
          this._row_headings[0] = this._col_headings[0] = content;
        else if (node.scope === "row" || this._col === 1)
          this._row_headings[this._row - 1] = content;
        else
          this._col_headings[this._col - 1] = content;
        break;
      
      case 'CAPTION':
        this._title = content;
        break;
    }
  },
  
  _ignore: function(node) {
    if (!this._options.except) return false;
    
    var content = this._strip_tags(node.innerHTML),
        classes = (node.className || '').split(/\s+/),
        list = [].concat(this._options.except);
    
    if (Bluff.index(list, content) >= 0) return true;
    var i = classes.length;
    while (i--) {
      if (Bluff.index(list, classes[i]) >= 0) return true;
    }
    return false;
  },
  
  _cleanup: function() {
    var i = this._skip_cols.length, index;
    while (i--) {
      index = this._skip_cols[i];
      if (index <= this._col_offset) continue;
      this._col_headings.splice(index - 1, 1);
      if (index >= this._col_offset)
        this._data.splice(index - 1 - this._col_offset, 1);
    }
    
    var i = this._skip_rows.length, index;
    while (i--) {
      index = this._skip_rows[i];
      if (index <= this._row_offset) continue;
      this._row_headings.splice(index - 1, 1);
      Bluff.each(this._data, function(series) {
        if (index >= this._row_offset)
          series.points.splice(index - 1 - this._row_offset, 1);
      }, this);
    }
  },
  
  _orient: function() {
    switch (this._orientation) {
      case 'auto':
        if ((this._row_headings.length > 1 && this._col_headings.length === 1) ||
            this._row_headings.length < this._col_headings.length) {
          this._transpose();
        }
        break;
        
      case 'rows':
        this._transpose();
        break;
    }
  },
  
  // Transpose data in memory
  _transpose: function() {
    var data = this._data, tmp;
    this._data = [];
    
    Bluff.each(data, function(row, i) {
      Bluff.each(row.points, function(point, p) {
        this.get_series(p).points[i] = point;
      }, this);
    }, this);
    
    tmp = this._row_headings;
    this._row_headings = this._col_headings;
    this._col_headings = tmp;
    
    tmp = this._row_offset;
    this._row_offset = this._col_offset;
    this._col_offset = tmp;
  },
  
  // Remove HTML from a string
  _strip_tags: function(string) {
    return string.replace(/<\/?[^>]+>/gi, '');
  },
  
  extend: {
    Mixin: new JS.Module({
      data_from_table: function(table, options) {
        var reader    = new Bluff.TableReader(table, options),
            data_rows = reader.get_data();
        
        Bluff.each(data_rows, function(row) {
          this.data(row.name, row.points);
        }, this);
        
        this.labels = reader.get_labels();
        this.title  = reader.get_title() || this.title;
      }
    })
  }
});

Bluff.Base.include(Bluff.TableReader.Mixin);