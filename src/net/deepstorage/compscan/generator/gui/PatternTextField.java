package net.deepstorage.compscan.generator.gui;

import java.awt.*;
import java.awt.event.*;
import javax.swing.*;
import javax.swing.text.*;
import javax.swing.event.*;

import java.util.regex.*;

public class PatternTextField extends JTextField{
   private final Matcher matcher; //events are thread-safe
   
   protected final AbstractDocument document;
   
   private final DocumentFilter filter=new DocumentFilter(){
      private final Segment segment=new Segment();
      private final StringBuilder buffer=new StringBuilder();
      
      private void copy() throws BadLocationException{
         segment.setPartialReturn(true);
         buffer.setLength(0);
         int left=document.getLength();
         int off=0;
         while(left>0){
            document.getText(off,left,segment);
            buffer.append(segment);
            off+=segment.count;
            left-=segment.count;
         }
      }
      
      public void insertString(
         DocumentFilter.FilterBypass fb, int offset, String string, AttributeSet attr
      ) throws BadLocationException{
         copy();
         buffer.insert(offset, string);
         matcher.reset(buffer);
         if(buffer.length()==0 || matcher.matches()) fb.insertString(offset, string, attr);
      }
      public void remove(
         DocumentFilter.FilterBypass fb, int offset, int length
      ) throws BadLocationException{
         copy();
         buffer.delete(offset, offset+length);
         matcher.reset(buffer);
         if(buffer.length()==0 || matcher.matches()) fb.remove(offset, length);
      }
      public void replace(
         DocumentFilter.FilterBypass fb, int offset, int length, 
         String text, AttributeSet attrs
      ) throws BadLocationException{
         copy();
         buffer.replace(offset, offset+length, text);
         matcher.reset(buffer);
         if(buffer.length()==0 || matcher.matches()) fb.replace(offset, length, text, attrs);
      }
   };
   
   public PatternTextField(String pattern, int columns){
      super(columns);
      matcher=Pattern.compile(pattern).matcher("");
      matcher.useAnchoringBounds(true);
      document=(AbstractDocument)getDocument();
      document.setDocumentFilter(filter);
      //document.addDocumentListener(listener);
   }
   
   public static void main(String[] args) {
      JFrame frame=new JFrame("test");
      frame.getContentPane().add(new PatternTextField("\\d+",10));
      frame.pack();
      frame.show();
   }   
}
