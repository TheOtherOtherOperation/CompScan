package net.deepstorage.compscan.generator.gui;

import java.util.*;
import java.util.regex.*;
import java.util.function.*;
import javax.swing.*;
import javax.swing.text.*;
import javax.swing.event.*;

public class NumberField extends PatternTextField{
   public interface Listener{
      public void valueChanged(Number value);
   }
   
   public static class Natural extends NumberField{
      public Natural(int columns){
         super("\\d+","\\d+",columns);
      }
   }
   
   public static class Real extends NumberField{
      public Real(int columns){
         super("[+-]?\\d*\\.?\\d*", "[+-]?\\d+(?:\\.\\d+)?",columns);
      }
   }
   
   public static class Ratio extends NumberField{
      public Ratio(int columns){
         super("1?(?:\\.0*)?|0?(?:\\.\\d*)?", "1(?:\\.0+)?|0(?:\\.\\d+)?",columns);
      }
   }
   
   public final List<Listener> listeners=new ArrayList<>();
   
   private String currentText;
   private final Matcher outputMatcher;
   
   private Number valueInstance=new Number(){
      public double doubleValue(){
         return Double.parseDouble(currentText);
      }
      public float floatValue(){
         return Float.parseFloat(currentText);
      }
      public int intValue(){
         return Integer.parseInt(currentText);
      }
      public long longValue(){
         return Long.parseLong(currentText);
      }
      
      public String toString(){
         return currentText;
      }
   };
   
   public Number currentValue;
   
   protected NumberField(
      String inputPattern, String outputPattern, int columns
   ){
      super(inputPattern,columns);
      outputMatcher=Pattern.compile(outputPattern).matcher("");
      document.addDocumentListener(new DocumentListener(){
         public void changedUpdate(DocumentEvent e){
            onChange();
         }
         public void insertUpdate(DocumentEvent e){
            onChange();
         }
         public void removeUpdate(DocumentEvent e){
            onChange();
         }
      });
   }
   
   private void onChange(){
      try{
         currentText=document.getText(0, document.getLength());
         outputMatcher.reset(currentText);
         currentValue= outputMatcher.matches()? valueInstance: null;
         for(int i=listeners.size(); i-->0;){
            listeners.get(i).valueChanged(currentValue);
         }
      }
      catch(BadLocationException e){
         //
      }
   }
   
   public static void main(String[] args) {
      JFrame frame=new JFrame("test");
      NumberField nf=new NumberField.Ratio(10);
      frame.getContentPane().add(nf);
      nf.listeners.add(v->System.out.println(v));
      frame.pack();
      frame.show();
   }   
}
