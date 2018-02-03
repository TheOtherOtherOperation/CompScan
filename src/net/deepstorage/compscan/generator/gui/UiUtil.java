package net.deepstorage.compscan.generator.gui;

import java.util.*;
import java.net.URL;
import java.text.*;
import java.util.Locale;
import java.util.regex.*;
import java.awt.*;
import static java.awt.GridBagConstraints.*;
import javax.swing.*;
import javax.swing.border.*;
import javax.swing.text.*;
import javax.swing.event.*;
import java.io.*;

public class UiUtil{
   public static final Frame NULL_FRAME=new Frame(){
      public void show(){}
   };
   
   //align:
   // - fill
   public static int FILL=2;
   
   private static final int[][] fillTable={
      {NONE,     HORIZONTAL},
      {VERTICAL, BOTH}
   };
   
   // - (-1,-1) -> SOUTHWEST; (1,1) -> NORTHEAST
   private static final int[][] alignTable={
      {SOUTHWEST, SOUTH,  SOUTHEAST},
      {WEST,      CENTER, EAST},
      {NORTHWEST, NORTH,  NORTHEAST},
   };
   
   private static final DateFormat FMT_DATE=new SimpleDateFormat("HH:mm:ss dd/MM/yyyy");
   private static final NumberFormat FMT_DOUBLE=new DecimalFormat(
      "#0.#", new DecimalFormatSymbols(Locale.US)
   );
   private static final NumberFormat FMT_INT=new DecimalFormat("#0");
   private final static Pattern P_DOUBLE=Pattern.compile(" *\\d*(?:\\.\\d*)? *");
   
   public static URL getResource(String path){
      return ClassLoader.getSystemClassLoader().getResource(path);
   }
   
   public static DocumentListener onAnyDocumentChange(Runnable r){
      return new DocumentListener(){
         public void changedUpdate(DocumentEvent e){
            r.run();
         }
         public void insertUpdate(DocumentEvent e){
            r.run();
         }
         public void removeUpdate(DocumentEvent e){
            r.run();
         }
      };
   }
   
   public static String format(Date d){
      return FMT_DATE.format(d);
   }
   
   public static String format(double d){
      return FMT_DOUBLE.format(d);
   }
   
   public static String format(int i){
      return FMT_INT.format(i);
   }
   
   public static boolean isDouble(String s){
      return P_DOUBLE.matcher(s).matches();
   }
   
   public static Component space(int w, int h){
      return Box.createRigidArea(new Dimension(w, h));
   }
   
   public static Component hgap(int v){
      return Box.createRigidArea(new Dimension(v, 0));
   }
   
   public static Component vgap(int v){
      return Box.createRigidArea(new Dimension(0, v));
   }
   
   public static Component hglue(){
      return Box.createHorizontalGlue();
   }
   
   public static Component vglue(){
      return Box.createVerticalGlue();
   }
   
   public static Component titledBox(String title, Component c){
      TitledBorder border=new TitledBorder(new EtchedBorder(), title);
      if(c instanceof JComponent){
         ((JComponent)c).setBorder(border);
         return c;
      }
      JPanel panel=new JPanel();
      panel.setLayout(new GridLayout(1,1));
      panel.add(c);
      panel.setBorder(border);
      return panel;
   }
   
   public static Component makeGrid(boolean vertical, Component[] comps){
      Container grid=new Container(){};
      grid.setLayout(vertical?
         new GridLayout(comps.length, 1):
         new GridLayout(1, comps.length)
      );
      for(int i=0;i<comps.length;i++){
         grid.add(comps[i]);
      }
      return grid;
   }
   
   public static Component makeHBox(
      float weightFirst, float weightMiddle, float weightLast, 
      final int margin, Component[] comps
   ){
      float[] weights=new float[comps.length];
      weights[0]=weightFirst;
      weights[comps.length-1]=weightLast;
      for(int i=1;i<(comps.length-1);i++) weights[i]=weightMiddle;
      return makeHBox(weights, margin, comps);
   }
   
   public static Component makeHBox(
      float[] weights, final int margin, Component[] comps
   ){
      Container box=new Container(){
         public Insets getInsets(){
            return new Insets(margin,margin,margin,margin);
         }
      };
      GridBagLayout gb=new GridBagLayout();
      box.setLayout(gb);
      GridBagConstraints gbc=new GridBagConstraints();
      gbc.gridwidth=1;
      gbc.gridheight=1;
      gbc.gridy=0;
      gbc.fill=gbc.BOTH;
      gbc.anchor=gbc.NORTH;
      gbc.weighty=1f;
      for(int i=0;i<comps.length;i++){
         gbc.gridx=i;
         gbc.weightx=weights[i];
         gb.setConstraints(comps[i], gbc);
         box.add(comps[i]);
      }
      return box;
   }
   
   public static Component makeVBox(
      float weightFirst, float weightMiddle, float weightLast, int margin, Component[] comps
   ){
      float[] weights=new float[comps.length];
      weights[0]=weightFirst;
      weights[comps.length-1]=weightLast;
      for(int i=1;i<(comps.length-1);i++) weights[i]=weightMiddle;
      return makeVBox(weights, margin, comps);
   }
   
   public static Component makeVBox(
      float[] weights, final int margin, Component[] comps
   ){
      Container box=new Container(){
         public Insets getInsets(){
            return new Insets(margin,margin,margin,margin);
         }
      };
      GridBagLayout gb=new GridBagLayout();
      box.setLayout(gb);
      GridBagConstraints gbc=new GridBagConstraints();
      gbc.gridwidth=1;//gbc.REMAINDER;
      gbc.gridheight=1;//gbc.RELATIVE;
      gbc.gridx=0;
      gbc.fill=gbc.BOTH;
      gbc.anchor=gbc.NORTH;
      gbc.weightx=1f;
      for(int i=0;i<comps.length;i++){
         gbc.gridy=i;
         gbc.weighty=weights[i];
         gb.setConstraints(comps[i], gbc);
         box.add(comps[i]);
      }
      return box;
   }
   
   public static Component makeGrid(
      float[] weightsX, float[] weightsY, boolean fillx, boolean filly,
      final int outerMargin, final int innerMargin, Component[][] comps
   ){
      int[] alignx=new int[comps[0].length];
      int[] aligny=new int[comps.length];
      for(int i=0;i<alignx.length;i++) alignx[i]= fillx? FILL: 0;
      for(int i=0;i<aligny.length;i++) aligny[i]= filly? FILL: 0;
      return makeGrid(weightsX, weightsY, alignx, aligny, outerMargin, innerMargin, comps);
   }
   
   //alignx - per column, -1=left, 0=center, 1=right
   //alignx - per row, -1=bottom, 0=center, 1=top
   public static Component makeGrid(
         float[] weightsX, float[] weightsY, 
         int[] alignx, int[] aligny,
         final int outerMargin, final int innerMargin, 
         Component[][] comps
   ){
      Container box=new Container(){
         public Insets getInsets(){
            return new Insets(outerMargin,outerMargin,outerMargin,outerMargin);
         }
      };
      GridBagLayout gb=new GridBagLayout();
      box.setLayout(gb);
      GridBagConstraints gbc=new GridBagConstraints();
      gbc.gridwidth=1;//gbc.REMAINDER;
      gbc.gridheight=1;//gbc.RELATIVE;
      Insets imargin=new Insets(innerMargin,innerMargin,innerMargin,innerMargin);
      for(int i=0;i<comps.length;i++){ //y
         Component[] row=comps[i];
         for(int j=0;j<row.length;j++){ //x
            gbc.gridy=i;
            gbc.gridx=j;
            gbc.weightx=weightsX[j];
            gbc.weighty=weightsY[i];
            gbc.ipadx=gbc.ipady=innerMargin;
            gbc.insets=imargin;
            int fillx=0, filly=0, alignX=alignx[j], alignY=aligny[i];
            if(alignx[j]==FILL){
               fillx=1;
               alignX=0;
            }
            if(alignY==FILL){
               filly=1;
               alignY=0;
            }
            gbc.fill=fillTable[filly][fillx];
            gbc.anchor=alignTable[alignY+1][alignX+1];
            gb.setConstraints(comps[i][j], gbc);
            box.add(comps[i][j]);
         }
      }
      return box;
   }
   
   public static Component makeTdlrGrid(
      Component[] comps, int maxRows, int maxCols, int rowSpan, int colSpan
   ){
      if(maxRows<0 && maxCols<0) throw new IllegalArgumentException();
      int rows= maxRows>0? maxRows: (comps.length+maxCols-1)/maxCols;
      int cols= maxCols>0? maxCols: (comps.length+maxRows-1)/maxRows;
      rows=(comps.length+cols-1)/cols;
      cols=(comps.length+rows-1)/rows;
      int gridHeight= rowSpan==0? rows: rows*2-1;
      int gridWidth= colSpan==0? cols: cols*2-1;
      Component[][] grid=new Component[gridHeight][gridWidth];
      int compInd=0;
      for(int col=0;col<gridWidth;col++){
         if(colSpan>0 && (col&1)==1){
            for(int row=0;row<gridHeight;row++){
               grid[row][col]=hgap(colSpan);
            }
            continue;
         }
         for(int row=0;row<gridHeight;row++){
            if(rowSpan>0 && (row&1)==1){
               grid[row][col]=vgap(rowSpan);
               continue;
            }
            grid[row][col]= compInd<comps.length?
               comps[compInd++]:
               hglue()
            ;
         }
      }
      float[] weightsX=new float[grid[0].length];
      float[] weightsY=new float[grid.length];
      return makeGrid(
         weightsX, weightsY, true, true, 0, 0, grid
      );
   }
   
   public static void errMsg(Component c, String context, String msg){
      JOptionPane.showMessageDialog(
         c, msg, context, JOptionPane.ERROR_MESSAGE
      );
   }
   
   public static void infMsg(Component c, String context, String msg){
      JOptionPane.showMessageDialog(
         c, msg, context, JOptionPane.INFORMATION_MESSAGE
      );
   }
   
   public static void toCenter(Window w, Component parent){
      while(parent!=null && !(parent instanceof Window)) parent=parent.getParent();
      toCenter(w, (Window)parent);
   }
   
   public static void toCenter(Window w, Window parent){
      if(parent!=null && parent!=NULL_FRAME) toCenter(w, parent.getBounds());
      else toCenter(w);
   }
   
   public static void toCenter(Window w){
      toCenter(
         w,
         GraphicsEnvironment.getLocalGraphicsEnvironment().
         getDefaultScreenDevice().getDefaultConfiguration().
         getBounds()
      );
   }
   
   private static void toCenter(Window w, Rectangle screen){
      Dimension size=w.getPreferredSize();
      size.width=Math.min(size.width, screen.width);
      size.height=Math.min(size.height, screen.height);
      int optimalX=screen.x+(screen.width-size.width)/2;
      int optimalY=screen.y+(screen.height-size.height)/2;
      w.setBounds(
         optimalX, optimalY, size.width, size.height
      );
   }
   
   public static File requestDir(String title, File current){
      JFileChooser chooser = new JFileChooser();
      if(current==null) current=new File(
         AppConfig.getString(AppConfig.WORKING_DIR, ".")
      );
      chooser.setCurrentDirectory(current);
      chooser.setDialogTitle(title);
      chooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
      chooser.setAcceptAllFileFilterUsed(false);
      if(chooser.showOpenDialog(null) == JFileChooser.APPROVE_OPTION) {
         File cd=chooser.getSelectedFile();
         AppConfig.put(AppConfig.WORKING_DIR, cd.getAbsolutePath());
         return cd;
      }
      else {
         return null;
      }
   }
   
   public static File requestFile(Component parent, FilenameFilter ff, String defaultName){
      return requestFile(parent, ff, defaultName, AppConfig.WORKING_DIR);
   }
   
   public static File requestFile(Component parent, FilenameFilter ff, String defaultName, String cfgParam){
//System.out.println("UIUtil.requestFile(..,"+defaultName+","+cfgParam+")");
      Frame frame=getFrame(parent);
      if(frame==null) frame=NULL_FRAME;
      FileDialog fd=new FileDialog(frame);
      fd.setFilenameFilter(ff);
      fd.setDirectory(new File(AppConfig.getString(cfgParam, ".")).getAbsolutePath());
      fd.setFile(defaultName);
      fd.show();
      String fname=fd.getFile();
      if(fname==null) return null;
      String dir=fd.getDirectory();
      AppConfig.put(cfgParam, dir);
      return new File(dir+"/"+fname);
   }
   
   public static Double requestDouble(Component parent, String msg, Double initial){
      for(;;){
         String s=JOptionPane.showInputDialog(parent,msg,initial);
         if(s==null) return initial;
         try{
            return Double.parseDouble(s);
         }
         catch(Exception e){
            infMsg(parent, msg, "Bad input format");
         }
      }
   }
   
   public static Frame getFrame(Component comp){
      while(comp!=null){
         if(comp instanceof Frame) return (Frame)comp;
         else return getFrame(comp.getParent());
      }
      return null;
   }
   
   public static void main(String[] args)throws Exception{
      System.out.println(requestDouble(null, "New value", 120.0));
      System.exit(0);
      
      //System.out.println(format(1.12345678));
      JLabel[][] labels=new JLabel[][]{
         {new JLabel("aa"), new JLabel("bbb"), new JLabel("cccc")},
         {new JLabel("ddddd"), new JLabel("eeeeee"), new JLabel("fffff")},
         {new JLabel("gggg"), new JLabel("hhh"), new JLabel("ii")}
      };
      for(int i=0;i<3;i++) for(int j=0;j<3;j++){
         labels[i][j].setBackground(Color.blue);
         labels[i][j].setOpaque(true);
      }
      //Component grid=makeTdlrGrid(labels, 3, 4, 0, 0);
      Component grid=makeGrid(new float[]{0,0,0}, new float[]{0,0,0}, 
         new int[]{-1,0,1}, new int[]{-1,0,1},
         0, 5, labels
      );
      JFrame frame=new JFrame("");
      frame.getContentPane().add(grid);
      frame.pack();
      frame.show();
   }
}